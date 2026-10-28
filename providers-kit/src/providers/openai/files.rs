// OpenAI file lifecycle stays provider-owned through private OpenAiClient file
// operations. Upload/list/retrieve/delete/download are direct ProviderBackend
// operations, while file-as-input compatibility for Responses and Batch rows is
// owned by model generate/batch paths. Do not expose OpenAI file clients or
// native file APIs publicly. Source:
// https://developers.openai.com/api/reference/resources/files
use reqwest::Method;
use reqwest::multipart::Form;
use std::num::NonZeroUsize;

use crate::KitError;
use crate::core::{limit_after_path, multipart_file_part};
use crate::error::ValidationViolation;
use crate::error::{ErrorPhase, ResponseViolation};
use crate::files::{FileListRequest, FileObject, FilePage, FileState, FileUploadRequest};
use crate::generation::FileReference;
use crate::transport::CheckedRequestSpec;
use crate::transport::{HttpResponse, RequestExecution};

use super::api::{OPENAI_REQUESTS, OpenAiApiContext, epoch_to_string};
use super::wire::{OpenAiDeleteObject, OpenAiFileObject, OpenAiList};

pub(crate) async fn upload_file(
    api: &OpenAiApiContext,
    request: FileUploadRequest,
) -> Result<FileObject, KitError> {
    let (file_name, media_type, data) = request.into_parts();
    upload_file_bytes(
        api,
        file_name,
        media_type,
        data,
        "user_data",
        api.transport().execution(),
    )
    .await?
    .normalize(normalize_file)
}

pub(crate) async fn list_files(
    api: &OpenAiApiContext,
    request: FileListRequest,
) -> Result<FilePage, KitError> {
    let (limit, cursor) = request.into_parts();
    let path = limit_after_path("files", limit, cursor.as_deref());
    let listed = OPENAI_REQUESTS
        .json::<OpenAiList<OpenAiFileObject>>(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().execution(),
        )
        .await?;
    listed.normalize(|listed| {
        Ok(FilePage {
            next_cursor: listed.required_next_cursor("OpenAI files list")?,
            files: listed
                .data
                .into_iter()
                .map(normalize_file)
                .collect::<Result<Vec<_>, _>>()?,
        })
    })
}

pub(crate) async fn file_info(
    api: &OpenAiApiContext,
    file_id: String,
) -> Result<FileObject, KitError> {
    validate_file_id(&file_id)?;
    let path = format!("files/{file_id}");
    let file = OPENAI_REQUESTS
        .json::<OpenAiFileObject>(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().execution(),
        )
        .await?;
    file.normalize(|file| {
        if file.id != file_id {
            return Err(
                KitError::invalid_response(ResponseViolation::IdentityMismatch { field: "id" })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message("OpenAI file lookup returned a different file id".to_string())
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(&file)),
            );
        }
        normalize_file(file)
    })
}

pub(crate) async fn delete_file(api: &OpenAiApiContext, file_id: String) -> Result<(), KitError> {
    validate_file_id(&file_id)?;
    let path = format!("files/{file_id}");
    let deleted = OPENAI_REQUESTS
        .json::<OpenAiDeleteObject>(
            &path,
            api.request(Method::DELETE, &path)?,
            api.transport().execution(),
        )
        .await?;
    deleted.normalize(|deleted| {
        if deleted.id != file_id || !deleted.deleted {
            return Err(
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "deletion_confirmation",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message("OpenAI file deletion did not confirm the requested file".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&deleted)),
            );
        }
        Ok(())
    })
}

pub(crate) async fn download_file(
    api: &OpenAiApiContext,
    file_id: String,
    max_bytes: NonZeroUsize,
) -> Result<Vec<u8>, KitError> {
    download_file_bytes(api, &file_id, max_bytes.get(), api.transport().execution())
        .await
        .map(HttpResponse::into_value)
}

pub(crate) async fn upload_file_bytes(
    api: &OpenAiApiContext,
    file_name: String,
    media_type: String,
    data: Vec<u8>,
    purpose: &str,
    execution: RequestExecution,
) -> Result<HttpResponse<OpenAiFileObject>, KitError> {
    let file_part = multipart_file_part(file_name, media_type, data)?;
    let form = Form::new()
        .text("purpose", purpose.to_string())
        .part("file", file_part);
    OPENAI_REQUESTS
        .json::<OpenAiFileObject>(
            "files",
            api.request(Method::POST, "files")?.multipart(form),
            execution,
        )
        .await
}

pub(crate) async fn download_file_bytes(
    api: &OpenAiApiContext,
    file_id: &str,
    max_bytes: usize,
    execution: RequestExecution,
) -> Result<HttpResponse<Vec<u8>>, KitError> {
    validate_file_id(file_id)?;
    let path = format!("files/{file_id}/content");
    OPENAI_REQUESTS
        .bytes_with_limit(
            &path,
            api.request(Method::GET, &path)?,
            execution,
            max_bytes,
        )
        .await
}

pub(super) fn validate_file_id(file_id: &str) -> Result<(), KitError> {
    let valid = file_id.strip_prefix("file-").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    });
    if !valid {
        return Err(KitError::invalid_input(
            ValidationViolation::InvalidFormat {
                field: "file.id",
                rule: "exact file- resource identifier",
            },
            "OpenAI file id must use the exact `file-...` resource shape",
        ));
    }
    Ok(())
}

pub(crate) fn normalize_file(file: OpenAiFileObject) -> Result<FileObject, KitError> {
    if validate_file_id(&file.id).is_err() {
        return Err(
            KitError::invalid_response(ResponseViolation::InvalidField { field: "id" })
                .at_phase(ErrorPhase::Normalize)
                .with_message("OpenAI file response included an invalid resource id".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&file)),
        );
    }
    let state = match file.status.as_deref() {
        Some("uploaded") => Some(FileState::Processing),
        Some("processed") => Some(FileState::Active),
        Some("error") => Some(FileState::Failed {
            code: None,
            message: None,
        }),
        Some(other) => {
            return Err(
                KitError::invalid_response(ResponseViolation::UnknownVariant { field: "status" })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(format!("OpenAI returned unknown file status `{other}`"))
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(&file)),
            );
        }
        None => None,
    };
    let input_reference = Some(FileReference::Provider {
        id: file.id.clone(),
        media_type: file.mime_type.clone(),
    });
    Ok(FileObject {
        id: file.id,
        file_name: file.filename,
        size_bytes: file.bytes,
        media_type: file.mime_type,
        created_at: epoch_to_string(file.created_at),
        state,
        input_reference,
    })
}
