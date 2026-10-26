use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use reqwest::Method;
use serde_json::{Map, Value, json};
use url::Url;

use crate::SdkError;
use crate::error::ValidationViolation;
use crate::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, JsonValueKind, NativeErrorDetails, RemoteFailure,
    ResponseDetails, ResponseViolation,
};
use crate::transport::HttpResponse;

use crate::batch::{
    BatchJob, BatchListRequest, BatchPage, BatchResult, BatchStatus, BatchSubmitRequest,
};
use crate::generation::GenerationContinuation;
use crate::transport::CheckedRequestSpec;

use super::api::{ANTHROPIC_REQUESTS, AnthropicApiContext};
use super::messages::{
    ClaudeThinkingProfile, build_message_body, normalize_generate_response, usage_from_anthropic,
};
use super::wire::{
    AnthropicBatchResultLine, AnthropicErrorEnvelope, AnthropicList, AnthropicMessageBatch,
};

pub(crate) fn render_batch_submission(
    model: &str,
    thinking_profile: ClaudeThinkingProfile,
    request: &BatchSubmitRequest<GenerationContinuation>,
) -> Result<Value, SdkError> {
    request.validate()?;
    let BatchSubmitRequest::Inline { rows } = request else {
        return Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "batch.file_id",
                rule: "file-backed input unavailable",
            },
            "Anthropic file-backed batch input",
        ));
    };
    let requests = rows
        .iter()
        .map(|row| {
            let mut params =
                build_message_body(model, thinking_profile, row.request().as_view(), false)?;
            if let Value::Object(ref mut object) = params {
                object.remove("stream");
            }
            Ok(json!({"custom_id": row.custom_id(), "params": params}))
        })
        .collect::<Result<Vec<_>, SdkError>>()?;
    Ok(json!({"requests": requests}))
}

pub(crate) async fn submit_rendered_batch(
    api: &AnthropicApiContext,
    body: Value,
) -> Result<BatchJob, SdkError> {
    let batch = ANTHROPIC_REQUESTS
        .json::<AnthropicMessageBatch>(
            "v1/messages/batches",
            api.request(Method::POST, "v1/messages/batches")?
                .json(&body),
            api.transport().execution(),
        )
        .await?;
    batch.normalize(AnthropicMessageBatch::into_job)
}

pub(crate) async fn batch_status(
    api: &AnthropicApiContext,
    batch_id: String,
) -> Result<BatchJob, SdkError> {
    let batch = fetch_batch(api, &batch_id).await?;
    batch.normalize(|batch| {
        if batch.id != batch_id {
            return Err(
                SdkError::invalid_response(ResponseViolation::IdentityMismatch {
                    field: "batch.id",
                })
                .with_message("Anthropic batch lookup returned a different batch id".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&batch)),
            );
        }
        batch.into_job()
    })
}

pub(crate) async fn cancel_batch(
    api: &AnthropicApiContext,
    batch_id: String,
) -> Result<BatchJob, SdkError> {
    validate_batch_id(&batch_id)?;
    let path = format!("v1/messages/batches/{batch_id}/cancel");
    let batch = ANTHROPIC_REQUESTS
        .json::<AnthropicMessageBatch>(
            &path,
            api.request(Method::POST, &path)?
                .json(&Map::<String, Value>::new()),
            api.transport().execution(),
        )
        .await?;
    batch.normalize(|batch| {
        if batch.id != batch_id {
            return Err(
                SdkError::invalid_response(ResponseViolation::IdentityMismatch {
                    field: "batch.id",
                })
                .with_message(
                    "Anthropic batch cancellation returned a different batch id".to_string(),
                )
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&batch)),
            );
        }
        batch.into_job()
    })
}

pub(crate) async fn batch_results(
    api: &AnthropicApiContext,
    batch_id: String,
    max_bytes: NonZeroUsize,
) -> Result<Vec<BatchResult<GenerationContinuation>>, SdkError> {
    let received = fetch_batch(api, &batch_id).await?;
    let path = received.normalize(|batch| {
        if batch.id != batch_id {
            return Err(
                SdkError::invalid_response(ResponseViolation::IdentityMismatch {
                    field: "batch.id",
                })
                .with_message(
                    "Anthropic batch results lookup returned a different batch id".to_string(),
                )
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&batch)),
            );
        }
        let path = match batch.results_url.as_deref() {
            Some(value) if !value.trim().is_empty() => {
                let url = Url::parse(value).map_err(|err| {
                    SdkError::invalid_response(ResponseViolation::InvalidField {
                        field: "results_url",
                    })
                    .with_message(format!("Anthropic batch results URL was invalid: {err}"))
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(value))
                })?;
                if !matches!(url.scheme(), "http" | "https")
                    || !url.username().is_empty()
                    || url.password().is_some()
                {
                    return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                        field: "results_url",
                    })
                    .with_message(
                        "Anthropic batch results URL was not a safe HTTP(S) URL".to_string(),
                    )
                    .with_diagnostic(None));
                }
                url
            }
            _ if batch.processing_status.as_deref() == Some("ended") => {
                return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                    field: "results_url",
                })
                .with_message("ended Anthropic batch did not include results_url".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&batch)));
            }
            _ => {
                return Err(SdkError::invalid_state(
                    "Anthropic batch results are not ready",
                ));
            }
        };
        Ok(path)
    })?;
    let path_text = path.to_string();
    let bytes = ANTHROPIC_REQUESTS
        .bytes_with_limit(
            &path_text,
            api.request_absolute(Method::GET, path),
            api.transport().execution(),
            max_bytes.get(),
        )
        .await?;
    bytes.normalize(|bytes| {
        let text = String::from_utf8(bytes).map_err(|err| {
            SdkError::invalid_response(ResponseViolation::InvalidUtf8)
                .at_phase(ErrorPhase::Decode)
                .with_message(format!("Anthropic batch output was not valid UTF-8: {err}"))
                .with_diagnostic(None)
        })?;
        AnthropicBatchResultLine::parse_results(&text)
    })
}

pub(crate) async fn list_batches(
    api: &AnthropicApiContext,
    request: BatchListRequest,
) -> Result<BatchPage, SdkError> {
    let (limit, cursor) = request.into_parts();
    let mut path = "v1/messages/batches".to_string();
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
    let list = ANTHROPIC_REQUESTS
        .json::<AnthropicList<AnthropicMessageBatch>>(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().execution(),
        )
        .await?;
    list.normalize(|list| {
        let next_cursor = list.validated_after_id("Anthropic batch list")?;
        Ok(BatchPage {
            jobs: list
                .data
                .into_iter()
                .map(AnthropicMessageBatch::into_job)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor,
        })
    })
}

async fn fetch_batch(
    api: &AnthropicApiContext,
    batch_id: &str,
) -> Result<HttpResponse<AnthropicMessageBatch>, SdkError> {
    validate_batch_id(batch_id)?;
    let path = format!("v1/messages/batches/{batch_id}");
    ANTHROPIC_REQUESTS
        .json(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().execution(),
        )
        .await
}

fn validate_batch_id(batch_id: &str) -> Result<(), SdkError> {
    let valid = batch_id.strip_prefix("msgbatch_").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    });
    if !valid {
        return Err(SdkError::invalid_input(
            ValidationViolation::InvalidFormat {
                field: "batch.id",
                rule: "exact msgbatch_ resource identifier",
            },
            "Anthropic batch id must use the exact `msgbatch_...` resource shape",
        ));
    }
    Ok(())
}

impl AnthropicBatchResultLine {
    fn parse_results(text: &str) -> Result<Vec<BatchResult<GenerationContinuation>>, SdkError> {
        let mut ids = BTreeSet::new();
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .enumerate()
            .map(|(row, line)| {
                let mut row_id = None;
                let result = (|| {
                    let raw: Value = serde_json::from_str(line).map_err(|err| {
                        SdkError::invalid_response(ResponseViolation::MalformedJson)
                            .with_response_details(ResponseDetails::json(&err))
                            .with_message(format!(
                                "failed to decode Anthropic batch result row: {err}"
                            ))
                            .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                                line.as_bytes(),
                            )))
                            .at_phase(ErrorPhase::Decode)
                    })?;
                    row_id = raw
                        .get("custom_id")
                        .and_then(Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                        .map(str::to_string);
                    let decoded: AnthropicBatchResultLine = serde_json::from_value(raw.clone())
                        .map_err(|err| {
                            SdkError::invalid_response(ResponseViolation::InvalidField {
                                field: "result",
                            })
                            .with_response_details(ResponseDetails::json(&err))
                            .with_message(format!(
                                "failed to decode Anthropic batch result object: {err}"
                            ))
                            .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                                line.as_bytes(),
                            )))
                            .at_phase(ErrorPhase::Decode)
                        })?;
                    let custom_id = decoded
                        .custom_id
                        .filter(|value| !value.trim().is_empty())
                        .ok_or_else(|| {
                            SdkError::invalid_response(ResponseViolation::MissingField {
                                field: "custom_id",
                            })
                            .with_response_details(ResponseDetails::FieldType {
                                expected: JsonValueKind::String,
                                actual: JsonValueKind::of(raw.get("custom_id")),
                            })
                            .with_message(
                                "Anthropic batch result row did not include custom_id".to_string(),
                            )
                            .with_diagnostic(Some(
                                CheckedRequestSpec::diagnostic_body(line.as_bytes()),
                            ))
                        })?;
                    if !ids.insert(custom_id.clone()) {
                        return Err(SdkError::invalid_response(
                            ResponseViolation::ProtocolViolation {
                                rule: "batch result custom ids must be unique",
                            },
                        )
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(&raw)));
                    }
                    let kind = decoded
                        .result
                        .get("type")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            SdkError::invalid_response(ResponseViolation::MissingField {
                                field: "result.type",
                            })
                            .with_response_details(ResponseDetails::FieldType {
                                expected: JsonValueKind::String,
                                actual: JsonValueKind::of(decoded.result.get("type")),
                            })
                            .with_message(
                                "Anthropic batch result row did not include result.type"
                                    .to_string(),
                            )
                            .with_diagnostic(Some(
                                CheckedRequestSpec::diagnostic_body(line.as_bytes()),
                            ))
                        })?;
                    match kind {
                        "succeeded" => {
                            let message = decoded.result.get("message").ok_or_else(|| {
                                SdkError::invalid_response(ResponseViolation::MissingField {
                                    field: "result.message",
                                })
                                .with_message(
                                    "Anthropic successful batch row did not include message"
                                        .to_string(),
                                )
                                .with_diagnostic(Some(
                                    CheckedRequestSpec::diagnostic_body(line.as_bytes()),
                                ))
                            })?;
                            Ok(BatchResult::Succeeded {
                                custom_id,
                                output: Box::new(normalize_generate_response(None, message)?),
                            })
                        }
                        "errored" => {
                            let error = decoded.result.get("error").ok_or_else(|| {
                                SdkError::invalid_response(ResponseViolation::MissingField {
                                    field: "result.error",
                                })
                                .with_message(
                                    "Anthropic failed batch row did not include error".to_string(),
                                )
                                .with_diagnostic(Some(
                                    CheckedRequestSpec::diagnostic_body(line.as_bytes()),
                                ))
                            })?;
                            let envelope: AnthropicErrorEnvelope =
                                serde_json::from_value(error.clone()).map_err(|decode| {
                                    SdkError::invalid_response(ResponseViolation::InvalidField {
                                        field: "result.error",
                                    })
                                    .with_response_details(ResponseDetails::json(&decode))
                                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(error))
                                    .at_phase(ErrorPhase::Decode)
                                })?;
                            if envelope.error.message.trim().is_empty() {
                                return Err(SdkError::invalid_response(
                                    ResponseViolation::InvalidField {
                                        field: "result.error.error.message",
                                    },
                                )
                                .with_response_details(ResponseDetails::FieldType {
                                    expected: JsonValueKind::String,
                                    actual: JsonValueKind::String,
                                })
                                .with_diagnostic(CheckedRequestSpec::diagnostic_json(error)));
                            }
                            Ok(BatchResult::Failed {
                                custom_id,
                                error: envelope
                                    .remote_failure(CheckedRequestSpec::diagnostic_json(error))?,
                            })
                        }
                        "canceled" => Ok(BatchResult::Cancelled { custom_id }),
                        "expired" => Ok(BatchResult::Expired { custom_id }),
                        other => Err(SdkError::invalid_response(
                            ResponseViolation::UnknownVariant {
                                field: "result.type",
                            },
                        )
                        .with_response_details(ResponseDetails::Discriminator {
                            actual: other.to_string(),
                        })
                        .with_diagnostic(Some(
                            CheckedRequestSpec::diagnostic_body(line.as_bytes()),
                        ))),
                    }
                })();
                result.map_err(|error: SdkError| {
                    let error = match row_id {
                        Some(id) => error.with_correlation_id(id),
                        None => error,
                    };
                    error.with_row(row)
                })
            })
            .collect()
    }
}

impl AnthropicMessageBatch {
    fn into_job(self) -> Result<BatchJob, SdkError> {
        let batch = self;
        let native = CheckedRequestSpec::diagnostic_json(&batch);
        if validate_batch_id(&batch.id).is_err() {
            return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                field: "batch.id",
            })
            .with_message("Anthropic batch response included an invalid resource id".to_string())
            .with_diagnostic(native));
        }
        let status = match batch.processing_status.as_deref() {
            Some("in_progress" | "canceling") => BatchStatus::Running,
            Some("ended") if batch.error.is_some() => BatchStatus::Failed {
                errors: vec![RemoteFailure::from_native(
                    ErrorKind::ProviderRejected,
                    ClassificationBasis::Unclassified,
                    NativeErrorDetails {
                        message: batch.error.clone(),
                        ..NativeErrorDetails::default()
                    },
                )?],
            },
            Some("ended") => BatchStatus::Completed,
            Some(other) => {
                return Err(
                    SdkError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "processing_status",
                    })
                    .with_response_details(ResponseDetails::Discriminator {
                        actual: other.to_string(),
                    })
                    .with_diagnostic(native),
                );
            }
            None => {
                return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                    field: "processing_status",
                })
                .with_message(
                    "Anthropic batch response did not include processing_status".to_string(),
                )
                .with_diagnostic(native));
            }
        };
        Ok(BatchJob {
            id: batch.id,
            status,
            created_at: batch.created_at,
            completed_at: batch.ended_at,
            usage: batch
                .usage
                .as_ref()
                .map(|usage| usage_from_anthropic(Some(usage))),
        })
    }
}
