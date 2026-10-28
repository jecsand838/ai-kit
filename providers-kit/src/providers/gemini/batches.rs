use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use reqwest::Method;
use serde::Deserialize;
use serde_json::{Map, Value};
use url::Url;

use crate::KitError;
use crate::error::ValidationViolation;
use crate::error::{
    ErrorPhase, JsonValueKind, NativeErrorDetails, Operation, ResponseDetails, ResponseViolation,
};
use crate::transport::HttpResponse;

use crate::batch::{
    BatchInput, BatchJob, BatchListRequest, BatchPage, BatchResult, BatchStatus, BatchSubmitRequest,
};
use crate::core::HostedToolMode;
use crate::generation::GenerationContinuation;
use crate::models::ModelId;
use crate::transport::CheckedRequestSpec;

use super::api::{GEMINI_REQUESTS, GeminiApiContext};
use super::generate_content::{build_generate_body, normalize_generate_response};
use super::reasoning::GeminiThinkingProfile;
use super::wire::{GeminiOperation, GeminiOperationsList, GeminiRpcStatus};

const BATCH_STATUS_BODY_LIMIT: NonZeroUsize = NonZeroUsize::new(32 * 1024 * 1024).unwrap();

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct GeminiBatchOperationMetadata {
    state: Option<String>,
    create_time: Option<String>,
    end_time: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct GeminiBatchOutput {
    inlined_responses: Option<GeminiBatchInlinedResponses>,
    responses_file: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiBatchInlinedResponses {
    inlined_responses: Vec<Value>,
}

enum GeminiBatchResultsSource {
    Inlined(Vec<Value>),
    ResponsesFile(String),
}

impl GeminiBatchResultsSource {
    async fn fetch(
        received: HttpResponse<Self>,
        api: &GeminiApiContext,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<BatchResult<GenerationContinuation>>, KitError> {
        match received.value() {
            Self::Inlined(items) => {
                Self::normalize_rows(items).map_err(|error| received.attach_error(error))
            }
            Self::ResponsesFile(file) => {
                super::files::validate_file_id(file)
                    .map_err(|error| received.attach_error(error))?;
                let mut url = api.absolute_url(&format!("download/v1beta/{file}:download"))?;
                url.query_pairs_mut().append_pair("alt", "media");
                Self::read_jsonl(api, url, max_bytes).await
            }
        }
    }
}

struct GeminiBatchSubmitPlan {
    path: String,
    body: Value,
}

impl GeminiBatchSubmitPlan {
    fn from_request(
        api: &GeminiApiContext,
        model: &ModelId,
        thinking: GeminiThinkingProfile,
        request: &BatchSubmitRequest<GenerationContinuation>,
    ) -> Result<Self, KitError> {
        request.validate()?;
        let input = match request {
            BatchSubmitRequest::Inline { rows } => Self::inline_input(api, model, thinking, rows)?,
            BatchSubmitRequest::ProviderFile { file_id } => {
                super::files::validate_file_id(file_id)?;
                let mut input = Map::new();
                input.insert("fileName".to_string(), Value::String(file_id.clone()));
                Value::Object(input)
            }
        };
        let mut batch = Map::new();
        batch.insert(
            "displayName".to_string(),
            Value::String("ai-kit batch".to_string()),
        );
        batch.insert("inputConfig".to_string(), input);
        let mut body = Map::new();
        body.insert("batch".to_string(), Value::Object(batch));
        Ok(Self {
            path: format!("v1beta/{model}:batchGenerateContent"),
            body: Value::Object(body),
        })
    }

    fn inline_input(
        api: &GeminiApiContext,
        model: &ModelId,
        thinking: GeminiThinkingProfile,
        rows: &[BatchInput<GenerationContinuation>],
    ) -> Result<Value, KitError> {
        let requests = rows
            .iter()
            .map(|row| {
                let mut metadata = Map::new();
                metadata.insert(
                    "custom_id".to_string(),
                    Value::String(row.custom_id().to_string()),
                );
                let mut item = Map::new();
                item.insert(
                    "request".to_string(),
                    build_generate_body(
                        api,
                        model,
                        thinking,
                        row.request().as_view(),
                        HostedToolMode::Generate,
                    )?,
                );
                item.insert("metadata".to_string(), Value::Object(metadata));
                Ok(Value::Object(item))
            })
            .collect::<Result<Vec<_>, KitError>>()?;
        let mut request_list = Map::new();
        request_list.insert("requests".to_string(), Value::Array(requests));
        let mut input = Map::new();
        input.insert("requests".to_string(), Value::Object(request_list));
        Ok(Value::Object(input))
    }
}

struct GeminiBatchOperationView<'a> {
    operation: &'a GeminiOperation,
}

impl<'a> GeminiBatchOperationView<'a> {
    fn new(operation: &'a GeminiOperation) -> Self {
        Self { operation }
    }

    fn metadata(&self) -> Result<GeminiBatchOperationMetadata, KitError> {
        let metadata = self.operation.metadata.as_ref().ok_or_else(|| {
            KitError::invalid_response(ResponseViolation::MissingField { field: "metadata" })
                .with_message("Gemini batch operation did not include metadata".to_string())
                .with_diagnostic(self.body())
        })?;
        serde_json::from_value(metadata.clone()).map_err(|err| {
            KitError::invalid_response(ResponseViolation::InvalidField { field: "metadata" })
                .with_response_details(ResponseDetails::json(&err))
                .with_message(format!("failed to decode Gemini batch metadata: {err}"))
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(metadata))
                .at_phase(ErrorPhase::Decode)
        })
    }

    fn status(&self) -> Result<BatchStatus, KitError> {
        let metadata = self.metadata()?;
        let state = metadata.state.as_deref().ok_or_else(|| {
            KitError::invalid_response(ResponseViolation::MissingField {
                field: "metadata.state",
            })
            .with_message("Gemini batch operation did not include state".to_string())
            .with_diagnostic(self.body())
        })?;
        Ok(match state {
            "JOB_STATE_PENDING" | "BATCH_STATE_PENDING" => BatchStatus::Queued,
            "JOB_STATE_RUNNING" | "BATCH_STATE_RUNNING" => BatchStatus::Running,
            "JOB_STATE_SUCCEEDED" | "BATCH_STATE_SUCCEEDED" => BatchStatus::Completed,
            "JOB_STATE_FAILED" | "BATCH_STATE_FAILED" => BatchStatus::Failed {
                errors: self
                    .operation
                    .error
                    .as_ref()
                    .map(|error| error.remote_failure(CheckedRequestSpec::diagnostic_json(error)))
                    .transpose()?
                    .into_iter()
                    .collect(),
            },
            "JOB_STATE_CANCELLED" | "BATCH_STATE_CANCELLED" => BatchStatus::Cancelled,
            "JOB_STATE_EXPIRED" | "BATCH_STATE_EXPIRED" => BatchStatus::Expired,
            other => {
                return Err(
                    KitError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "metadata.state",
                    })
                    .with_response_details(ResponseDetails::Discriminator {
                        actual: other.to_string(),
                    })
                    .with_diagnostic(self.body()),
                );
            }
        })
    }

    fn job(&self) -> Result<BatchJob, KitError> {
        if validate_batch_id(&self.operation.name).is_err() {
            return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                field: "name",
            })
            .with_message("Gemini batch operation included an invalid resource name".to_string())
            .with_diagnostic(self.body()));
        }
        let metadata = self.metadata()?;
        Ok(BatchJob {
            id: self.operation.name.clone(),
            status: self.status()?,
            created_at: metadata.create_time,
            completed_at: metadata.end_time,
            usage: None,
        })
    }

    fn results_source(&self) -> Result<Option<GeminiBatchResultsSource>, KitError> {
        let Some(response) = self.operation.response.as_ref() else {
            return Ok(None);
        };
        let output: GeminiBatchOutput =
            serde_json::from_value(response.clone()).map_err(|err| {
                KitError::invalid_response(ResponseViolation::InvalidField { field: "response" })
                    .with_response_details(ResponseDetails::json(&err))
                    .with_message(format!("failed to decode Gemini batch output: {err}"))
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(response))
                    .at_phase(ErrorPhase::Decode)
            })?;
        match (output.inlined_responses, output.responses_file) {
            (Some(_), Some(_)) => Err(KitError::invalid_response(
                ResponseViolation::ProtocolViolation {
                    rule: "batch output must have only one result source",
                },
            )
            .with_message("Gemini batch output included two result sources".to_string())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(response))),
            (Some(value), None) => Ok(Some(GeminiBatchResultsSource::Inlined(
                value.inlined_responses,
            ))),
            (None, Some(value)) if value.trim().is_empty() => {
                Err(KitError::invalid_response(ResponseViolation::InvalidField {
                    field: "responsesFile",
                })
                .with_message("Gemini batch output included an empty responsesFile".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(response)))
            }
            (None, Some(value)) => Ok(Some(GeminiBatchResultsSource::ResponsesFile(value))),
            (None, None) => Ok(None),
        }
    }

    fn body(&self) -> Option<NativeErrorDetails> {
        CheckedRequestSpec::diagnostic_json(self.operation)
    }
}

pub(crate) fn render_batch_submission(
    api: &GeminiApiContext,
    model: &ModelId,
    thinking: GeminiThinkingProfile,
    request: &BatchSubmitRequest<GenerationContinuation>,
) -> Result<(String, Value), KitError> {
    let plan = GeminiBatchSubmitPlan::from_request(api, model, thinking, request)?;
    Ok((plan.path, plan.body))
}

pub(crate) async fn submit_rendered_batch(
    api: &GeminiApiContext,
    path: String,
    body: Value,
) -> Result<BatchJob, KitError> {
    let operation = GEMINI_REQUESTS
        .json::<GeminiOperation>(
            &path,
            api.request(Method::POST, &path)?.json(&body),
            api.transport().execution(),
        )
        .await?;
    operation.normalize(|operation| GeminiBatchOperationView::new(&operation).job())
}

pub(crate) async fn batch_status(
    api: &GeminiApiContext,
    batch_id: String,
) -> Result<BatchJob, KitError> {
    let operation = fetch_operation(api, &batch_id, BATCH_STATUS_BODY_LIMIT).await?;
    operation.normalize(|operation| {
        if operation.name != batch_id {
            return Err(
                KitError::invalid_response(ResponseViolation::IdentityMismatch { field: "name" })
                    .with_message(
                        "Gemini batch lookup returned a different operation id".to_string(),
                    )
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(&operation)),
            );
        }
        GeminiBatchOperationView::new(&operation).job()
    })
}

pub(crate) async fn cancel_batch(
    api: &GeminiApiContext,
    batch_id: String,
) -> Result<BatchJob, KitError> {
    validate_batch_id(&batch_id)?;
    let path = format!("v1beta/{batch_id}:cancel");
    GEMINI_REQUESTS
        .bytes(
            &path,
            api.request(Method::POST, &path)?
                .json(&Map::<String, Value>::new()),
            api.transport().execution(),
        )
        .await?;
    batch_status(api, batch_id).await
}

pub(crate) async fn batch_results(
    api: &GeminiApiContext,
    batch_id: String,
    max_bytes: NonZeroUsize,
) -> Result<Vec<BatchResult<GenerationContinuation>>, KitError> {
    let received = fetch_operation(api, &batch_id, max_bytes).await?;
    let source = received.try_map(|operation| {
        if operation.name != batch_id {
            return Err(
                KitError::invalid_response(ResponseViolation::IdentityMismatch { field: "name" })
                    .with_message(
                        "Gemini batch results lookup returned a different operation id".to_string(),
                    )
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(&operation)),
            );
        }
        let view = GeminiBatchOperationView::new(&operation);
        match view.results_source()? {
            Some(source) => Ok(source),
            None => match view.status()? {
                BatchStatus::Completed => {
                    Err(KitError::invalid_response(ResponseViolation::MissingField {
                        field: "response",
                    })
                    .with_message(
                        "completed Gemini batch did not include a result source".to_string(),
                    )
                    .with_diagnostic(view.body()))
                }
                BatchStatus::Failed { errors } => {
                    let failure = errors.into_iter().next().ok_or_else(|| {
                        KitError::invalid_response(ResponseViolation::MissingField {
                            field: "error",
                        })
                        .with_diagnostic(view.body())
                    })?;
                    Err(KitError::from_remote(
                        failure,
                        Operation::BatchResults,
                        ErrorPhase::Normalize,
                    )
                    .with_request_target(operation.name.clone()))
                }
                BatchStatus::Cancelled => Err(KitError::invalid_state(
                    "Gemini batch was cancelled without a result source",
                )),
                BatchStatus::Expired => Err(KitError::invalid_state(
                    "Gemini batch expired without a result source",
                )),
                BatchStatus::Queued | BatchStatus::Running => Err(KitError::invalid_state(
                    "Gemini batch results are not ready",
                )),
            },
        }
    })?;
    GeminiBatchResultsSource::fetch(source, api, max_bytes).await
}

pub(crate) async fn list_batches(
    api: &GeminiApiContext,
    request: BatchListRequest,
) -> Result<BatchPage, KitError> {
    let (limit, cursor) = request.into_parts();
    let url = api.absolute_url_with_page(
        "v1beta/batches",
        limit.map(std::num::NonZeroUsize::get),
        cursor.as_deref(),
    )?;
    let response = GEMINI_REQUESTS
        .json::<GeminiOperationsList>(
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
                .with_message("Gemini batch list returned an empty next page token".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&response)));
            }
            Some(cursor) => Some(cursor.to_string()),
            None => None,
        };
        Ok(BatchPage {
            jobs: response
                .operations
                .iter()
                .map(|operation| GeminiBatchOperationView::new(operation).job())
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor,
        })
    })
}

async fn fetch_operation(
    api: &GeminiApiContext,
    batch_id: &str,
    max_bytes: NonZeroUsize,
) -> Result<HttpResponse<GeminiOperation>, KitError> {
    validate_batch_id(batch_id)?;
    let path = format!("v1beta/{batch_id}");
    let bytes = GEMINI_REQUESTS
        .bytes_with_limit(
            &path,
            api.request(Method::GET, &path)?,
            api.transport().retryable_execution(),
            max_bytes.get(),
        )
        .await?;
    bytes.try_map(|bytes| {
        serde_json::from_slice(&bytes).map_err(|err| {
            KitError::invalid_response(ResponseViolation::from_json_error(&err, "operation"))
                .with_response_details(ResponseDetails::json(&err))
                .with_message(format!("failed to decode Gemini batch operation: {err}"))
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(&bytes)))
                .at_phase(ErrorPhase::Decode)
        })
    })
}

fn validate_batch_id(batch_id: &str) -> Result<(), KitError> {
    let valid = batch_id.strip_prefix("batches/").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    });
    if !valid {
        return Err(KitError::invalid_input(
            ValidationViolation::InvalidFormat {
                field: "batch.id",
                rule: "exact batches/ resource identifier",
            },
            "Gemini batch id must use the exact `batches/...` resource shape",
        ));
    }
    Ok(())
}

struct GeminiBatchRow<'a> {
    value: &'a Value,
}

impl<'a> GeminiBatchRow<'a> {
    fn violation(&self, violation: ResponseViolation) -> KitError {
        KitError::invalid_response(violation)
            .at_phase(ErrorPhase::Normalize)
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(self.value))
    }

    fn custom_id(&self) -> Result<&'a str, KitError> {
        let mut identity: Option<&str> = None;
        for value in [
            self.value.pointer("/metadata/custom_id"),
            self.value.get("key"),
            self.value.get("custom_id"),
        ]
        .into_iter()
        .flatten()
        {
            let id = value
                .as_str()
                .filter(|id| !id.trim().is_empty())
                .ok_or_else(|| {
                    self.violation(ResponseViolation::InvalidField { field: "custom_id" })
                        .with_response_details(ResponseDetails::FieldType {
                            expected: JsonValueKind::String,
                            actual: JsonValueKind::of(Some(value)),
                        })
                })?;
            if let Some(previous) = identity
                && previous != id
            {
                return Err(self
                    .violation(ResponseViolation::IdentityMismatch { field: "custom_id" })
                    .with_response_details(ResponseDetails::Identity {
                        expected: previous.to_string(),
                        actual: id.to_string(),
                    }));
            }
            identity = Some(id);
        }
        identity.ok_or_else(|| {
            self.violation(ResponseViolation::MissingField { field: "custom_id" })
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::String,
                    actual: JsonValueKind::Absent,
                })
        })
    }

    fn normalize(&self, custom_id: &str) -> Result<BatchResult<GenerationContinuation>, KitError> {
        let response = self.value.get("response").filter(|value| !value.is_null());
        let error = self.value.get("error").filter(|value| !value.is_null());
        match (response, error) {
            (Some(response), None) => {
                let version = response.get("modelVersion").ok_or_else(|| {
                    self.violation(ResponseViolation::MissingField {
                        field: "modelVersion",
                    })
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::String,
                        actual: JsonValueKind::Absent,
                    })
                })?;
                let model = version
                    .as_str()
                    .and_then(|value| ModelId::new(value.to_string()).ok())
                    .ok_or_else(|| {
                        self.violation(ResponseViolation::InvalidField {
                            field: "modelVersion",
                        })
                        .with_response_details(
                            ResponseDetails::FieldType {
                                expected: JsonValueKind::String,
                                actual: JsonValueKind::of(Some(version)),
                            },
                        )
                    })?;
                Ok(BatchResult::Succeeded {
                    custom_id: custom_id.to_string(),
                    output: Box::new(normalize_generate_response(&model, response.clone())?),
                })
            }
            (None, Some(error)) => {
                let status: GeminiRpcStatus =
                    serde_json::from_value(error.clone()).map_err(|decode| {
                        self.violation(ResponseViolation::InvalidField { field: "error" })
                            .with_response_details(ResponseDetails::json(&decode))
                            .at_phase(ErrorPhase::Decode)
                    })?;
                if status.code == Some(0)
                    || status.status.as_deref() == Some("OK")
                    || (status.code.is_none() && status.status.as_deref().is_none_or(str::is_empty))
                    || status
                        .message
                        .as_deref()
                        .is_none_or(|message| message.trim().is_empty())
                {
                    return Err(self.violation(ResponseViolation::ProtocolViolation {
                        rule: "failed batch row must contain an RPC status and message",
                    }));
                }
                Ok(BatchResult::Failed {
                    custom_id: custom_id.to_string(),
                    error: status.remote_failure(CheckedRequestSpec::diagnostic_json(error))?,
                })
            }
            (Some(_), Some(_)) | (None, None) => {
                Err(self.violation(ResponseViolation::ProtocolViolation {
                    rule: "batch row must contain exactly one response or error",
                }))
            }
        }
    }
}

impl GeminiBatchResultsSource {
    fn normalize_rows(
        items: &[Value],
    ) -> Result<Vec<BatchResult<GenerationContinuation>>, KitError> {
        let mut ids = BTreeSet::new();
        items
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let row = GeminiBatchRow { value };
                let id = row.custom_id().map_err(|error| error.with_row(index))?;
                if !ids.insert(id) {
                    return Err(row
                        .violation(ResponseViolation::ProtocolViolation {
                            rule: "batch result custom ids must be unique",
                        })
                        .with_correlation_id(id.to_string())
                        .with_row(index));
                }
                row.normalize(id)
                    .map_err(|error| error.with_correlation_id(id.to_string()).with_row(index))
            })
            .collect()
    }

    async fn read_jsonl(
        api: &GeminiApiContext,
        url: Url,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<BatchResult<GenerationContinuation>>, KitError> {
        let path = url.to_string();
        let received = GEMINI_REQUESTS
            .bytes_with_limit(
                &path,
                api.request_absolute(Method::GET, url, true),
                api.transport().execution(),
                max_bytes.get(),
            )
            .await?;
        received.normalize(|bytes| {
            let text = std::str::from_utf8(&bytes).map_err(|_| {
                KitError::invalid_response(ResponseViolation::InvalidUtf8)
                    .at_phase(ErrorPhase::Decode)
                    .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(&bytes)))
            })?;
            let items = text
                .lines()
                .filter(|line| !line.trim().is_empty())
                .enumerate()
                .map(|(index, line)| {
                    serde_json::from_str::<Value>(line).map_err(|error| {
                        KitError::invalid_response(ResponseViolation::MalformedJson)
                            .with_response_details(ResponseDetails::json(&error))
                            .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                                line.as_bytes(),
                            )))
                            .at_phase(ErrorPhase::Decode)
                            .with_row(index)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Self::normalize_rows(&items)
        })
    }
}
