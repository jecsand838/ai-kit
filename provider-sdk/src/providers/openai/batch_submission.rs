//! Ownership of an internally uploaded batch input through creation settlement.

use reqwest::Method;
use serde_json::json;

use crate::SdkError;
use crate::batch::{BatchJob, BatchSubmitRequest};
use crate::batch_ownership::BatchReceipt;
use crate::error::{
    ErrorKind, ErrorPhase, NativeDetail, NativeErrorDetails, Operation, ResponseViolation,
};
use crate::generation::GenerationContinuation;
use crate::models::ModelId;
use crate::transport::RequestExecution;

use super::api::{OPENAI_REQUESTS, OpenAiApiContext};
use super::batches::{OpenAiBatchSubmitPlan, render_batch_submission, validate_batch_id};
use super::files::{delete_file, upload_file_bytes, validate_file_id};
use super::wire::OpenAiBatchObject;

pub(crate) struct OpenAiBatchSubmission {
    api: OpenAiApiContext,
    jsonl: String,
    execution: RequestExecution,
}

impl OpenAiBatchSubmission {
    pub(crate) fn new(
        api: OpenAiApiContext,
        model: &ModelId,
        request: &BatchSubmitRequest<GenerationContinuation>,
    ) -> Result<Self, SdkError> {
        match render_batch_submission(model, request)? {
            OpenAiBatchSubmitPlan::InlineUpload { jsonl } => Self::from_rendered(api, jsonl),
            OpenAiBatchSubmitPlan::ProviderFile { .. } => Err(SdkError::invalid_state(
                "borrowed batch input cannot enter inline ownership",
            )),
        }
    }

    pub(super) fn from_rendered(api: OpenAiApiContext, jsonl: String) -> Result<Self, SdkError> {
        let execution = api.transport().execution();
        let deadline = execution.deadline()?;
        Ok(Self {
            api,
            jsonl,
            execution: execution.through_deadline(deadline),
        })
    }

    pub(crate) fn payload_bytes(&self) -> usize {
        self.jsonl.len()
    }

    pub(crate) async fn execute(mut self, receipt: &BatchReceipt) -> Result<BatchJob, SdkError> {
        if receipt.abandoned() {
            return Err(Self::cancelled());
        }
        let execution = self.execution.clone();
        let deadline = execution.deadline()?;
        let uploaded = upload_file_bytes(
            &self.api,
            "batch-input.jsonl".to_owned(),
            "application/jsonl".to_owned(),
            std::mem::take(&mut self.jsonl).into_bytes(),
            "batch",
            execution.clone().through_deadline(deadline),
        )
        .await?;
        let file_id = uploaded.normalize(|file| {
            validate_file_id(&file.id).map_err(|_| {
                SdkError::invalid_response(ResponseViolation::InvalidField { field: "id" })
                    .with_message(
                        "OpenAI upload returned an invalid input file identity".to_owned(),
                    )
            })?;
            Ok(file.id)
        })?;
        // This checkpoint precedes every subsequent await; even destruction of
        // the task's runtime can still report the exact input it learned.
        let retained = Self::retained(&file_id, None);
        receipt.checkpoint(Some(retained.clone()));
        if receipt.abandoned() {
            return Err(self.cleanup(&file_id, receipt, Self::cancelled()).await);
        }
        let body = json!({ "input_file_id": file_id, "endpoint": "/v1/responses", "completion_window": "24h" });
        let request = match self.api.request(Method::POST, "batches") {
            Ok(request) => request.json(&body),
            Err(error) => return Err(self.cleanup(&file_id, receipt, error).await),
        };
        if let Err(error) = execution.remaining_timeout(deadline) {
            return Err(self.cleanup(&file_id, receipt, error).await);
        }
        // From this point observer loss never cancels creation. A transport
        // failure, malformed 2xx, or ambiguous HTTP rejection retains the input.
        let created = OPENAI_REQUESTS
            .json::<OpenAiBatchObject>("batches", request, execution.through_deadline(deadline))
            .await;
        match created {
            Err(error) if Self::definitely_not_created(&error) => {
                Err(self.cleanup(&file_id, receipt, error).await)
            }
            Err(error) => Err(error.with_supplementary(retained)),
            Ok(created) => {
                let batch_id = validate_batch_id(&created.value().id)
                    .is_ok()
                    .then_some(created.value().id.as_str());
                let retained = Self::retained(&file_id, batch_id);
                receipt.checkpoint(Some(retained.clone()));
                created
                    .normalize(OpenAiBatchObject::into_job)
                    .map_err(|error| error.with_supplementary(retained))
            }
        }
        .map_err(|error| error.at_operation(Operation::SubmitBatch))
    }

    fn definitely_not_created(error: &SdkError) -> bool {
        error.context().phase == ErrorPhase::RequestBuild
            || (error.context().phase == ErrorPhase::ResponseHeaders
                && matches!(error.http_status(), Some(400 | 422)))
    }

    async fn cleanup(&self, file: &str, receipt: &BatchReceipt, primary: SdkError) -> SdkError {
        // Cleanup gets its own configured bounded request budget. No retries,
        // inferred IDs, borrowed inputs, or accepted jobs enter this path.
        match delete_file(&self.api, file.to_owned()).await {
            Ok(()) => {
                receipt.checkpoint(None);
                primary
            }
            Err(cleanup) => primary.with_supplementary(
                cleanup
                    .at_operation(Operation::DeleteFile)
                    .at_phase(ErrorPhase::Cleanup)
                    .with_request_target(format!("files/{file}")),
            ),
        }
    }

    fn cancelled() -> SdkError {
        SdkError::new(
            ErrorKind::Cancelled,
            Operation::SubmitBatch,
            ErrorPhase::RequestSend,
        )
        .with_message("inline batch observer was dropped before job creation".to_owned())
    }

    fn retained(file: &str, batch: Option<&str>) -> SdkError {
        let message = match batch {
            Some(batch) => format!(
                "unobserved batch submission retained input `{file}` and returned batch `{batch}`"
            ),
            None => format!(
                "batch submission retained input `{file}` without an observed batch identity"
            ),
        };
        SdkError::invalid_state("inline batch resources require caller attention")
            .at_operation(Operation::SubmitBatch)
            .at_phase(ErrorPhase::Cleanup)
            .with_request_target(format!("files/{file}"))
            .with_native_capture(NativeErrorDetails {
                message: Some(message),
                details: std::iter::once(file)
                    .chain(batch)
                    .map(|name| NativeDetail::Resource {
                        resource_type: None,
                        name: name.to_owned(),
                        owner: None,
                        description: None,
                    })
                    .collect(),
                ..NativeErrorDetails::default()
            })
    }
}
