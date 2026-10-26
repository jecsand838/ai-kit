use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use reqwest::Method;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::SdkError;
use crate::batch::{
    BatchInput, BatchJob, BatchListRequest, BatchPage, BatchResult, BatchStatus, BatchSubmitRequest,
};
use crate::batch_ownership::BatchReceipt;
use crate::core::limit_after_path;
use crate::error::ValidationViolation;
use crate::error::{
    ErrorPhase, JsonValueKind, NativeErrorDetails, RemoteFailure, ResponseDetails,
    ResponseViolation,
};
use crate::generation::GenerationContinuation;
use crate::models::ModelId;
use crate::transport::CheckedRequestSpec;
use crate::transport::HttpResponse;
use crate::transport::RequestExecution;

use super::api::{OPENAI_REQUESTS, OpenAiApiContext, epoch_to_string};
use super::batch_submission::OpenAiBatchSubmission;
use super::files::{download_file_bytes, validate_file_id};
use super::models::OpenAiModel;
use super::responses::normalize_generate_response;
use super::wire::{OpenAiBatchObject, OpenAiErrorObject, OpenAiList};

pub(crate) enum OpenAiBatchSubmitPlan {
    InlineUpload { jsonl: String },
    ProviderFile { file_id: String, model: ModelId },
}

#[derive(Deserialize)]
struct OpenAiBatchInputRow {
    custom_id: String,
    method: String,
    url: String,
    body: OpenAiBatchInputBody,
}

#[derive(Deserialize)]
struct OpenAiBatchInputBody {
    model: String,
}

impl OpenAiBatchSubmitPlan {
    // SDK memory bound; the service's separately documented 200 MB admission
    // limit remains authoritative. No caller file is rewritten or re-uploaded.
    const MAX_INPUT_BYTES: usize = 200 * 1024 * 1024;
    const MAX_ROWS: usize = 50_000;

    fn inline_jsonl(
        model: &ModelId,
        requests: &[BatchInput<GenerationContinuation>],
    ) -> Result<String, SdkError> {
        if requests.len() > Self::MAX_ROWS {
            return Err(SdkError::invalid_input(
                ValidationViolation::LimitExceeded {
                    field: "batch.rows",
                    limit: Self::MAX_ROWS,
                },
                "OpenAI batch input exceeds the row bound",
            ));
        }
        let mut jsonl = String::new();
        for row in requests {
            let body = OpenAiModel::build_response_body(model, row.request().as_view(), false)?;
            let line = serde_json::to_string(&json!({
                "custom_id": row.custom_id(), "method": "POST", "url": "/v1/responses", "body": body,
            })).map_err(|error| SdkError::invalid_state(format!("failed to encode validated OpenAI batch row: {error}")))?;
            let separator = usize::from(!jsonl.is_empty());
            if line.len()
                > Self::MAX_INPUT_BYTES
                    .saturating_sub(jsonl.len())
                    .saturating_sub(separator)
            {
                return Err(SdkError::invalid_input(
                    ValidationViolation::LimitExceeded {
                        field: "batch.bytes",
                        limit: Self::MAX_INPUT_BYTES,
                    },
                    "OpenAI batch input exceeds the encoded byte bound",
                ));
            }
            if separator != 0 {
                jsonl.push('\n');
            }
            jsonl.push_str(&line);
        }
        Ok(jsonl)
    }

    fn validate_file_rows(model: &ModelId, bytes: &[u8]) -> Result<(), SdkError> {
        let text = std::str::from_utf8(bytes).map_err(|_| {
            SdkError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "batch.file",
                    rule: "UTF-8 JSONL",
                },
                "OpenAI batch input must contain UTF-8 JSONL",
            )
        })?;
        let mut ids = BTreeSet::new();
        for (index, line) in text.lines().enumerate() {
            if index >= Self::MAX_ROWS {
                return Err(SdkError::invalid_input(
                    ValidationViolation::LimitExceeded {
                        field: "batch.rows",
                        limit: Self::MAX_ROWS,
                    },
                    "OpenAI batch input exceeds the row bound",
                )
                .with_row(index));
            }
            let row: OpenAiBatchInputRow = serde_json::from_str(line).map_err(|_| {
                SdkError::invalid_input(
                    ValidationViolation::InvalidFormat {
                        field: "batch.row",
                        rule: "one unambiguous batch request per JSONL row",
                    },
                    "OpenAI batch row is malformed or omits required binding fields",
                )
                .with_row(index)
            })?;
            row.validate(model).map_err(|error| error.with_row(index))?;
            if !ids.insert(row.custom_id) {
                return Err(SdkError::invalid_input(
                    ValidationViolation::Duplicate {
                        field: "batch.custom_id",
                    },
                    "OpenAI batch row identifiers must be unique",
                )
                .with_row(index));
            }
        }
        if ids.is_empty() {
            return Err(SdkError::invalid_input(
                ValidationViolation::Empty {
                    field: "batch.rows",
                },
                "OpenAI batch input must contain at least one row",
            ));
        }
        Ok(())
    }
}

impl OpenAiBatchInputRow {
    fn validate(&self, model: &ModelId) -> Result<(), SdkError> {
        for (field, valid, rule) in [
            (
                "batch.custom_id",
                !self.custom_id.trim().is_empty(),
                "nonempty row identifier",
            ),
            ("batch.method", self.method == "POST", "POST"),
            (
                "batch.url",
                self.url == "/v1/responses",
                "the Responses endpoint",
            ),
            (
                "batch.model",
                self.body.model == model.as_str(),
                "the exact selected model",
            ),
        ] {
            if !valid {
                return Err(SdkError::invalid_input(
                    ValidationViolation::InvalidValue { field, rule },
                    format!("OpenAI batch row must use {rule}"),
                ));
            }
        }
        Ok(())
    }
}

pub(crate) fn render_batch_submission(
    model: &ModelId,
    request: &BatchSubmitRequest<GenerationContinuation>,
) -> Result<OpenAiBatchSubmitPlan, SdkError> {
    request.validate()?;
    OpenAiModel::validate_stateless_model(model)?;
    match request {
        BatchSubmitRequest::Inline { rows } => Ok(OpenAiBatchSubmitPlan::InlineUpload {
            jsonl: OpenAiBatchSubmitPlan::inline_jsonl(model, rows)?,
        }),
        BatchSubmitRequest::ProviderFile { file_id } => {
            validate_file_id(file_id)?;
            Ok(OpenAiBatchSubmitPlan::ProviderFile {
                file_id: file_id.clone(),
                model: model.clone(),
            })
        }
    }
}

pub(crate) async fn submit_rendered_batch(
    api: &OpenAiApiContext,
    plan: OpenAiBatchSubmitPlan,
) -> Result<BatchJob, SdkError> {
    let execution = api.transport().execution();
    let deadline = execution.deadline()?;
    let input_file_id = match plan {
        OpenAiBatchSubmitPlan::InlineUpload { jsonl } => {
            // The supplied Model transfers this concrete operation to the
            // Provider runtime. Private trait dispatch uses the same native
            // algorithm, never a second upload/create implementation.
            return OpenAiBatchSubmission::from_rendered(api.clone(), jsonl)?
                .execute(&BatchReceipt::new())
                .await;
        }
        OpenAiBatchSubmitPlan::ProviderFile { file_id, model } => {
            let bytes = download_file_bytes(
                api,
                &file_id,
                OpenAiBatchSubmitPlan::MAX_INPUT_BYTES,
                execution.clone().through_deadline(deadline),
            )
            .await?
            .into_value();
            OpenAiBatchSubmitPlan::validate_file_rows(&model, &bytes)?;
            file_id
        }
    };
    let body = json!({
        "input_file_id": input_file_id,
        "endpoint": "/v1/responses",
        "completion_window": "24h",
    });
    let batch = OPENAI_REQUESTS
        .json::<OpenAiBatchObject>(
            "batches",
            api.request(Method::POST, "batches")?.json(&body),
            execution.through_deadline(deadline),
        )
        .await?;
    batch.normalize(OpenAiBatchObject::into_job)
}

pub(crate) async fn cancel_batch(
    api: &OpenAiApiContext,
    batch_id: String,
) -> Result<BatchJob, SdkError> {
    validate_batch_id(&batch_id)?;
    let path = format!("batches/{batch_id}/cancel");
    let batch = OPENAI_REQUESTS
        .json::<OpenAiBatchObject>(
            &path,
            api.request(Method::POST, &path)?
                .json(&Map::<String, Value>::new()),
            api.transport().execution(),
        )
        .await?;
    batch.normalize(|batch| {
        if batch.id != batch_id {
            return Err(
                SdkError::invalid_response(ResponseViolation::IdentityMismatch { field: "id" })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(
                        "OpenAI batch cancellation returned a different batch id".to_string(),
                    )
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(&batch)),
            );
        }
        batch.into_job()
    })
}

pub(crate) async fn batch_results(
    api: &OpenAiApiContext,
    batch_id: String,
    max_bytes: NonZeroUsize,
) -> Result<Vec<BatchResult<GenerationContinuation>>, SdkError> {
    let received = api
        .fetch_batch(&batch_id, api.transport().execution())
        .await?;
    let files = received.normalize(|batch| {
        if batch.id != batch_id {
            return Err(
                SdkError::invalid_response(ResponseViolation::IdentityMismatch { field: "id" })
                    .at_phase(ErrorPhase::Normalize)
                    .with_response_details(ResponseDetails::Identity {
                        expected: batch_id.clone(),
                        actual: batch.id.clone(),
                    })
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(&batch)),
            );
        }
        let mut files = Vec::new();
        for (field, file) in [
            ("output_file_id", &batch.output_file_id),
            ("error_file_id", &batch.error_file_id),
        ] {
            if let Some(file) = file {
                if validate_file_id(file).is_err() {
                    return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                        field,
                    })
                    .with_response_details(ResponseDetails::Discriminator {
                        actual: file.clone(),
                    })
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(&batch)));
                }
                files.push(file.clone());
            }
        }
        if files.is_empty() {
            return if batch.status == "completed" {
                Err(
                    SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                        rule: "completed batch must identify a result file",
                    })
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(&batch)),
                )
            } else {
                Err(SdkError::invalid_state(
                    "OpenAI batch results are not ready",
                ))
            };
        }
        Ok(files)
    })?;
    let mut results = OpenAiBatchResults::new(max_bytes.get());
    for file in files {
        results.read_file(api, &file).await?;
    }
    Ok(results.rows)
}

pub(crate) async fn list_batches(
    api: &OpenAiApiContext,
    request: BatchListRequest,
) -> Result<BatchPage, SdkError> {
    let (limit, cursor) = request.into_parts();
    let path = limit_after_path("batches", limit, cursor.as_deref());
    let list = OPENAI_REQUESTS
        .json::<OpenAiList<OpenAiBatchObject>>(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().execution(),
        )
        .await?;
    list.normalize(|list| {
        Ok(BatchPage {
            next_cursor: list.required_next_cursor("OpenAI batch list")?,
            jobs: list
                .data
                .into_iter()
                .map(OpenAiBatchObject::into_job)
                .collect::<Result<Vec<_>, _>>()?,
        })
    })
}

impl OpenAiApiContext {
    pub(super) async fn fetch_batch(
        &self,
        batch_id: &str,
        execution: RequestExecution,
    ) -> Result<HttpResponse<OpenAiBatchObject>, SdkError> {
        validate_batch_id(batch_id)?;
        let path = format!("batches/{batch_id}");
        OPENAI_REQUESTS
            .json(&path, self.request(Method::GET, &path)?, execution)
            .await
    }
}

pub(super) fn validate_batch_id(batch_id: &str) -> Result<(), SdkError> {
    let valid = batch_id.strip_prefix("batch_").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    });
    if !valid {
        return Err(SdkError::invalid_input(
            ValidationViolation::InvalidFormat {
                field: "batch.id",
                rule: "exact batch_ resource identifier",
            },
            "OpenAI batch id must use the exact `batch_...` resource shape",
        ));
    }
    Ok(())
}

struct OpenAiBatchResults {
    rows: Vec<BatchResult<GenerationContinuation>>,
    ids: BTreeSet<String>,
    limit: usize,
    consumed: usize,
}

impl OpenAiBatchResults {
    fn new(limit: usize) -> Self {
        Self {
            rows: Vec::new(),
            ids: BTreeSet::new(),
            limit,
            consumed: 0,
        }
    }

    async fn read_file(&mut self, api: &OpenAiApiContext, file: &str) -> Result<(), SdkError> {
        let path = format!("files/{file}/content");
        let remaining = self.limit - self.consumed;
        let received = OPENAI_REQUESTS
            .bytes_with_limit(
                &path,
                api.request(Method::GET, &path)?,
                api.transport().execution(),
                remaining,
            )
            .await
            .map_err(|error| {
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::InvalidResponse(
                        ResponseViolation::LimitExceeded { .. }
                    )
                ) {
                    let observed = match error.response_details() {
                        Some(ResponseDetails::Size { observed }) => {
                            self.consumed.saturating_add(*observed)
                        }
                        _ => self.limit.saturating_add(1),
                    };
                    SdkError::invalid_response(ResponseViolation::LimitExceeded {
                        limit: self.limit,
                    })
                    .at_phase(ErrorPhase::ReadBody)
                    .with_response_details(ResponseDetails::Size { observed })
                    .with_native_details(error.native_details().clone())
                    .with_cause(error)
                } else {
                    error
                }
            })?;
        received.normalize(|bytes| {
            self.consumed += bytes.len();
            let text = std::str::from_utf8(&bytes).map_err(|_| {
                SdkError::invalid_response(ResponseViolation::InvalidUtf8)
                    .at_phase(ErrorPhase::Decode)
                    .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(&bytes)))
            })?;
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let row_index = self.rows.len();
                let value: Value = serde_json::from_str(line).map_err(|error| {
                    SdkError::invalid_response(ResponseViolation::MalformedJson)
                        .with_response_details(ResponseDetails::json(&error))
                        .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(line.as_bytes())))
                        .at_phase(ErrorPhase::Decode)
                        .with_row(row_index)
                })?;
                let row = OpenAiBatchRow { value: &value };
                let custom_id = row
                    .string("custom_id")
                    .map_err(|error| error.with_row(row_index))?;
                if !self.ids.insert(custom_id.to_string()) {
                    return Err(row
                        .violation(ResponseViolation::ProtocolViolation {
                            rule: "batch result custom ids must be unique",
                        })
                        .with_correlation_id(custom_id.to_string())
                        .with_row(row_index));
                }
                let result = row.normalize(custom_id).map_err(|error| {
                    error
                        .with_correlation_id(custom_id.to_string())
                        .with_row(row_index)
                })?;
                self.rows.push(result);
            }
            Ok(())
        })
    }
}

struct OpenAiBatchRow<'a> {
    value: &'a Value,
}

impl OpenAiBatchRow<'_> {
    fn violation(&self, violation: ResponseViolation) -> SdkError {
        SdkError::invalid_response(violation)
            .at_phase(ErrorPhase::Normalize)
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(self.value))
    }

    fn string(&self, field: &'static str) -> Result<&str, SdkError> {
        self.value
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                self.violation(if self.value.get(field).is_some() {
                    ResponseViolation::InvalidField { field }
                } else {
                    ResponseViolation::MissingField { field }
                })
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::String,
                    actual: JsonValueKind::of(self.value.get(field)),
                })
            })
    }

    fn failure(error: &Value, mut details: NativeErrorDetails) -> Result<RemoteFailure, SdkError> {
        if let Some(body) = CheckedRequestSpec::diagnostic_json(error) {
            details.body = body.body;
            details.truncated |= body.truncated;
        }
        let decoded: OpenAiErrorObject =
            serde_json::from_value(error.clone()).map_err(|error| {
                SdkError::invalid_response(ResponseViolation::InvalidField { field: "error" })
                    .with_response_details(ResponseDetails::json(&error))
                    .at_phase(ErrorPhase::Decode)
                    .with_native_details(details.clone())
            })?;
        if decoded.message.trim().is_empty() {
            return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                field: "error.message",
            })
            .with_response_details(ResponseDetails::FieldType {
                expected: JsonValueKind::String,
                actual: JsonValueKind::String,
            })
            .with_native_details(details));
        }
        decoded.remote_failure(Some(details))
    }

    fn normalize(&self, custom_id: &str) -> Result<BatchResult<GenerationContinuation>, SdkError> {
        if let Some(error) = self.value.get("error").filter(|error| !error.is_null()) {
            if self
                .value
                .get("response")
                .is_some_and(|response| !response.is_null())
            {
                return Err(self.violation(ResponseViolation::ProtocolViolation {
                    rule: "batch row must contain exactly one response or error",
                }));
            }
            return Ok(BatchResult::Failed {
                custom_id: custom_id.to_string(),
                error: Self::failure(error, NativeErrorDetails::default())?,
            });
        }
        let response = self
            .value
            .get("response")
            .filter(|value| value.is_object())
            .ok_or_else(|| {
                self.violation(ResponseViolation::InvalidField { field: "response" })
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::Object,
                        actual: JsonValueKind::of(self.value.get("response")),
                    })
            })?;
        let status = response
            .get("status_code")
            .and_then(Value::as_u64)
            .and_then(|status| u16::try_from(status).ok())
            .filter(|status| (100..600).contains(status))
            .ok_or_else(|| {
                self.violation(ResponseViolation::InvalidField {
                    field: "response.status_code",
                })
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::Number,
                    actual: JsonValueKind::of(response.get("status_code")),
                })
            })?;
        let body = response.get("body").ok_or_else(|| {
            self.violation(ResponseViolation::MissingField {
                field: "response.body",
            })
        })?;
        if !(200..300).contains(&status)
            || body.get("status").and_then(Value::as_str) == Some("failed")
        {
            let error = body.get("error").ok_or_else(|| {
                self.violation(ResponseViolation::MissingField {
                    field: "response.body.error",
                })
            })?;
            return Ok(BatchResult::Failed {
                custom_id: custom_id.to_string(),
                error: Self::failure(
                    error,
                    NativeErrorDetails {
                        reported_http_status: Some(status),
                        native_status: body
                            .get("status")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        request_id: response
                            .get("request_id")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        ..NativeErrorDetails::default()
                    },
                )?,
            });
        }
        Ok(BatchResult::Succeeded {
            custom_id: custom_id.to_string(),
            output: Box::new(normalize_generate_response(None, body)?),
        })
    }
}

impl OpenAiBatchObject {
    pub(super) fn into_job(self) -> Result<BatchJob, SdkError> {
        if validate_batch_id(&self.id).is_err() {
            return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                field: "id",
            })
            .with_response_details(ResponseDetails::Discriminator {
                actual: self.id.clone(),
            })
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(&self)));
        }
        let status = match self.status.as_str() {
            "validating" => BatchStatus::Queued,
            "in_progress" | "finalizing" | "cancelling" => BatchStatus::Running,
            "completed" => BatchStatus::Completed,
            "failed" => {
                let errors = match self.errors.as_ref().filter(|value| !value.is_null()) {
                    None => Vec::new(),
                    Some(value) => {
                        if !value.is_object() {
                            return Err(SdkError::invalid_response(
                                ResponseViolation::InvalidField { field: "errors" },
                            )
                            .with_response_details(ResponseDetails::FieldType {
                                expected: JsonValueKind::Object,
                                actual: JsonValueKind::of(Some(value)),
                            })
                            .with_diagnostic(CheckedRequestSpec::diagnostic_json(value)));
                        }
                        match value.get("data").filter(|data| !data.is_null()) {
                            None => Vec::new(),
                            Some(data) => {
                                let rows = data.as_array().ok_or_else(|| {
                                    SdkError::invalid_response(ResponseViolation::InvalidField {
                                        field: "errors.data",
                                    })
                                    .with_response_details(ResponseDetails::FieldType {
                                        expected: JsonValueKind::Array,
                                        actual: JsonValueKind::of(Some(data)),
                                    })
                                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(value))
                                })?;
                                rows.iter()
                                    .map(OpenAiBatchError::decode)
                                    .collect::<Result<Vec<_>, _>>()?
                            }
                        }
                    }
                };
                BatchStatus::Failed { errors }
            }
            "cancelled" => BatchStatus::Cancelled,
            "expired" => BatchStatus::Expired,
            other => {
                return Err(
                    SdkError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "status",
                    })
                    .with_response_details(ResponseDetails::Discriminator {
                        actual: other.to_string(),
                    })
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(&self)),
                );
            }
        };
        Ok(BatchJob {
            id: self.id,
            status,
            created_at: epoch_to_string(self.created_at),
            completed_at: epoch_to_string(self.completed_at),
            usage: None,
        })
    }
}

#[derive(Deserialize)]
struct OpenAiBatchError {
    code: Option<String>,
    message: Option<String>,
    param: Option<String>,
}

impl OpenAiBatchError {
    fn decode(value: &Value) -> Result<RemoteFailure, SdkError> {
        let error: Self = serde_json::from_value(value.clone()).map_err(|error| {
            SdkError::invalid_response(ResponseViolation::InvalidField {
                field: "errors.data",
            })
            .with_response_details(ResponseDetails::json(&error))
            .at_phase(ErrorPhase::Decode)
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(value))
        })?;
        OpenAiErrorObject::remote_details(NativeErrorDetails {
            code: error.code,
            message: error.message,
            parameter: error.param,
            ..CheckedRequestSpec::diagnostic_json(value).unwrap_or_default()
        })
    }
}
