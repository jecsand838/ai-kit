use std::collections::BTreeMap;

use provider_sdk::batch::{BatchInput, BatchResult, BatchStatus};
use provider_sdk::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, NativeDetail, NativeErrorDetails, Operation,
    RemoteFailure, ResponseViolation,
};
use provider_sdk::files::{FileObject, FilePage, FileState, FileUploadRequest};
use provider_sdk::generation::{FileReference, GenerateRequest, GenerationConfig, Message};

use crate::Fixture;
use crate::hosted_assertions::HostedProvider;
use crate::live_assertions::{
    BatchCleanup, BatchEvidence, DeletedFileEvidence, FileInventoryEvidence, GeminiOutputRetention,
    InlinePublication,
};

#[test]
fn fixture_file_readiness_requires_affirmative_gemini_active_state() {
    let request = DeletedFixture::upload_request();
    let uploaded = DeletedFixture::uploaded_file(&request);
    for (state, ready) in [
        (Some(FileState::Active), true),
        (Some(FileState::Processing), false),
        (None, false),
    ] {
        let mut observed = uploaded.clone();
        observed.state = state;
        assert_eq!(
            BatchEvidence::fixture_file_ready(
                &observed,
                &uploaded,
                &request,
                HostedProvider::Gemini,
            )
            .unwrap(),
            ready,
            "state {:?}",
            observed.state,
        );
    }
}

#[test]
fn fixture_file_readiness_preserves_other_providers_absent_metadata() {
    let request = DeletedFixture::upload_request();
    let mut uploaded = DeletedFixture::uploaded_file(&request);
    uploaded.state = None;
    uploaded.media_type = None;
    uploaded.size_bytes = None;
    uploaded.input_reference = Some(FileReference::Provider {
        id: uploaded.id.clone(),
        media_type: None,
    });
    for provider in [HostedProvider::OpenAi, HostedProvider::Anthropic] {
        assert!(
            BatchEvidence::fixture_file_ready(&uploaded, &uploaded, &request, provider).unwrap()
        );
    }
}

#[test]
fn fixture_file_readiness_checks_upload_and_lookup_against_request_metadata() {
    let request = DeletedFixture::upload_request();
    let uploaded = DeletedFixture::uploaded_file(&request);
    let mut malformed = Vec::new();
    for name in [None, Some("another-fixture.txt".to_owned())] {
        let mut file = uploaded.clone();
        file.file_name = name;
        malformed.push(("filename", file));
    }
    for media_type in [None, Some("application/json".to_owned())] {
        let mut file = uploaded.clone();
        file.media_type = media_type;
        malformed.push(("MIME", file));
    }
    for bytes in [None, Some(0), Some(request.data().len() as u64 - 1)] {
        let mut file = uploaded.clone();
        file.size_bytes = bytes;
        malformed.push(("byte length", file));
    }
    for (field, malformed) in malformed {
        assert!(
            BatchEvidence::fixture_file_ready(
                &malformed,
                &uploaded,
                &request,
                HostedProvider::Gemini,
            )
            .is_err(),
            "lookup {field} must be checked against the request",
        );
        assert!(
            BatchEvidence::fixture_file_ready(
                &uploaded,
                &malformed,
                &request,
                HostedProvider::Gemini,
            )
            .is_err(),
            "upload acknowledgment {field} must be checked against the request",
        );
        assert!(
            BatchEvidence::fixture_file_ready(
                &malformed,
                &malformed,
                &request,
                HostedProvider::Gemini,
            )
            .is_err(),
            "matching upload and lookup {field} cannot override the actual request",
        );
    }
}

#[test]
fn fixture_file_readiness_preserves_exact_identity_and_returned_url() {
    let request = DeletedFixture::upload_request();
    let uploaded = DeletedFixture::uploaded_file(&request);
    assert!(
        BatchEvidence::fixture_file_ready(&uploaded, &uploaded, &request, HostedProvider::Gemini,)
            .unwrap()
    );
    let reference = uploaded.input_reference.clone();
    for id in ["", " ", "files/different"] {
        let mut observed = uploaded.clone();
        observed.id = id.to_owned();
        assert!(
            BatchEvidence::fixture_file_ready(
                &observed,
                &uploaded,
                &request,
                HostedProvider::Gemini,
            )
            .is_err()
        );
    }
    let mut empty = uploaded.clone();
    empty.id.clear();
    assert!(
        BatchEvidence::fixture_file_ready(&empty, &empty, &request, HostedProvider::Gemini)
            .is_err()
    );
    assert_eq!(uploaded.input_reference, reference);
}

#[test]
fn fixture_file_readiness_requires_usable_unchanged_gemini_input_reference() {
    let request = DeletedFixture::upload_request();
    let uploaded = DeletedFixture::uploaded_file(&request);
    let mut references = vec![
        None,
        Some(FileReference::Provider {
            id: uploaded.id.clone(),
            media_type: Some(request.media_type().to_owned()),
        }),
    ];
    for (url, media_type) in [
        (
            "http://generativelanguage.googleapis.com/v1beta/files/owned",
            Some("text/plain"),
        ),
        (
            "https://user:password@generativelanguage.googleapis.com/v1beta/files/owned",
            Some("text/plain"),
        ),
        (
            "https://generativelanguage.googleapis.com/v1beta/files/other",
            Some("text/plain"),
        ),
        (
            "https://generativelanguage.googleapis.com/v1beta/files/owned",
            None,
        ),
        (
            "https://generativelanguage.googleapis.com/v1beta/files/owned",
            Some("application/json"),
        ),
    ] {
        references.push(Some(FileReference::Url {
            url: url.parse().unwrap(),
            media_type: media_type.map(str::to_owned),
        }));
    }
    for reference in references {
        let mut observed = uploaded.clone();
        observed.input_reference = reference;
        assert!(
            BatchEvidence::fixture_file_ready(
                &observed,
                &uploaded,
                &request,
                HostedProvider::Gemini,
            )
            .is_err(),
            "invalid or changed reference must not reach generation",
        );
    }
}

#[test]
fn fixture_file_readiness_validates_upload_reference_without_trusting_lookup() {
    let request = DeletedFixture::upload_request();
    for reference in [
        None,
        Some(FileReference::Url {
            url: "http://generativelanguage.googleapis.com/v1beta/files/owned"
                .parse()
                .unwrap(),
            media_type: Some("text/plain".into()),
        }),
    ] {
        let mut file = DeletedFixture::uploaded_file(&request);
        file.input_reference = reference;
        assert!(
            BatchEvidence::fixture_file_ready(&file, &file, &request, HostedProvider::Gemini)
                .is_err(),
            "a consistently invalid reference is not usable",
        );
    }
}

#[test]
fn fixture_file_readiness_keeps_native_processing_failure_details() {
    let request = DeletedFixture::upload_request();
    let uploaded = DeletedFixture::uploaded_file(&request);
    let mut observed = uploaded.clone();
    observed.state = Some(FileState::Failed {
        code: Some("DOCUMENT_PROCESSING_FAILED".into()),
        message: Some("fixture document could not be processed".into()),
    });
    let error =
        BatchEvidence::fixture_file_ready(&observed, &uploaded, &request, HostedProvider::Gemini)
            .unwrap_err();
    let evidence = error.evidence();
    assert_eq!(
        evidence["failures"][0]["remote"]["details"]["code"],
        "DOCUMENT_PROCESSING_FAILED"
    );
    assert_eq!(
        evidence["failures"][0]["remote"]["details"]["message"],
        "fixture document could not be processed"
    );
}

struct RetentionFixture;

impl RetentionFixture {
    const JOB: &str = "batches/abcdefghijklmnopqrstuvwxyz0123456789";
    const FILE: &str = "files/batch-abcdefghijklmnopqrstuvwxyz0123456789";
    const MESSAGE: &str = "* DeleteFileRequest.name: File ID (name excluding 'files/') cannot be more than 40 characters.\n";

    fn metadata() -> serde_json::Value {
        serde_json::json!({
            "name": Self::JOB,
            "metadata": { "state": "BATCH_STATE_SUCCEEDED", "endTime": "2026-09-23T00:00:00Z" },
            "response": { "responsesFile": Self::FILE },
        })
    }

    fn error() -> provider_sdk::SdkError {
        provider_sdk::SdkError::new(
            ErrorKind::InvalidRequest,
            provider_sdk::error::Operation::DeleteFile,
            provider_sdk::error::ErrorPhase::ResponseHeaders,
        )
        .with_http_status(400)
        .with_request_target(Self::FILE)
        .with_native_details(NativeErrorDetails {
            native_status: Some("INVALID_ARGUMENT".into()),
            reported_http_status: Some(400),
            message: Some(Self::MESSAGE.into()),
            ..Default::default()
        })
    }
}

#[test]
fn gemini_output_retention_preserves_error_and_never_claims_deletion() {
    for state in ["BATCH_STATE_SUCCEEDED", "JOB_STATE_SUCCEEDED"] {
        let mut metadata = RetentionFixture::metadata();
        metadata["metadata"]["state"] = state.into();
        let mut output =
            GeminiOutputRetention::from_metadata(RetentionFixture::JOB, &metadata).unwrap();
        assert!(!output.is_deferred());
        output.defer(RetentionFixture::error()).unwrap();
        assert!(output.is_deferred());
        let evidence = output.evidence();
        assert_eq!(evidence["id"], RetentionFixture::FILE);
        assert_eq!(evidence["batch_id"], RetentionFixture::JOB);
        assert_eq!(evidence["completed_at"], "2026-09-23T00:00:00Z");
        assert_eq!(evidence["cleanup_state"], "awaiting_provider_expiration");
        assert_eq!(evidence["deleted"], false);
        assert_eq!(evidence["expiration_verified"], false);
        assert_eq!(evidence["provider_retention_days"], 42);
        assert_eq!(
            evidence["deletion_error"]["sdk"]["native"]["message"],
            RetentionFixture::MESSAGE
        );
        assert!(
            output.defer(RetentionFixture::error()).is_err(),
            "a retained error must not be overwritten"
        );
    }
}

#[test]
fn gemini_output_retention_requires_exact_completed_output_metadata() {
    for (pointer, replacement) in [
        ("/name", serde_json::json!("batches/different")),
        ("/metadata/state", serde_json::json!("BATCH_STATE_RUNNING")),
        ("/metadata/state", serde_json::json!("BATCH_STATE_FAILED")),
        (
            "/metadata/state",
            serde_json::json!("BATCH_STATE_CANCELLED"),
        ),
        ("/metadata/state", serde_json::Value::Null),
        ("/metadata/endTime", serde_json::Value::Null),
        ("/metadata/endTime", serde_json::json!("")),
        ("/response/responsesFile", serde_json::json!("files/input")),
        (
            "/response/responsesFile",
            serde_json::json!(format!("files/{}", "a".repeat(40))),
        ),
        (
            "/response/responsesFile",
            serde_json::json!(format!("files/{}?", "a".repeat(41))),
        ),
        ("/response/responsesFile", serde_json::Value::Null),
    ] {
        let mut metadata = RetentionFixture::metadata();
        *metadata.pointer_mut(pointer).unwrap() = replacement;
        assert!(
            GeminiOutputRetention::from_metadata(RetentionFixture::JOB, &metadata).is_none(),
            "{pointer}"
        );
    }
    let mut metadata = RetentionFixture::metadata();
    metadata["error"] = serde_json::json!({"code": 3});
    assert!(GeminiOutputRetention::from_metadata(RetentionFixture::JOB, &metadata).is_none());
    assert!(
        GeminiOutputRetention::from_metadata("batches/unowned", &RetentionFixture::metadata())
            .is_none()
    );
}

#[test]
fn gemini_output_retention_rejects_unrelated_or_incomplete_errors() {
    use provider_sdk::error::{ErrorPhase, Operation};
    let mut errors = vec![
        RetentionFixture::error().with_http_status(403),
        RetentionFixture::error().with_request_target("files/different"),
        provider_sdk::SdkError::new(
            ErrorKind::InvalidRequest,
            Operation::FileInfo,
            ErrorPhase::ResponseHeaders,
        )
        .with_http_status(400)
        .with_request_target(RetentionFixture::FILE)
        .with_native_details(RetentionFixture::error().native_details().clone()),
        provider_sdk::SdkError::new(
            ErrorKind::InvalidRequest,
            Operation::DeleteFile,
            ErrorPhase::Cleanup,
        )
        .with_http_status(400)
        .with_request_target(RetentionFixture::FILE)
        .with_native_details(RetentionFixture::error().native_details().clone()),
        provider_sdk::SdkError::new(
            ErrorKind::Authentication,
            Operation::DeleteFile,
            ErrorPhase::ResponseHeaders,
        )
        .with_http_status(400)
        .with_request_target(RetentionFixture::FILE)
        .with_native_details(RetentionFixture::error().native_details().clone()),
    ];
    for kind in [
        ErrorKind::QuotaExceeded,
        ErrorKind::Transport,
        ErrorKind::Timeout,
    ] {
        errors.push(
            provider_sdk::SdkError::new(kind, Operation::DeleteFile, ErrorPhase::ResponseHeaders)
                .with_http_status(400)
                .with_request_target(RetentionFixture::FILE)
                .with_native_details(RetentionFixture::error().native_details().clone()),
        );
    }
    for details in [
        NativeErrorDetails {
            message: Some("another validation error".into()),
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails {
            message: Some(format!(
                "{}* Another validation failed.\n",
                RetentionFixture::MESSAGE
            )),
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails {
            native_status: Some("PERMISSION_DENIED".into()),
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails {
            reported_http_status: Some(403),
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails {
            truncated: true,
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails {
            omitted_details: 1,
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails::default(),
    ] {
        errors.push(RetentionFixture::error().with_native_details(details));
    }
    errors.push(RetentionFixture::error().with_supplementary(RetentionFixture::error()));
    errors.push(RetentionFixture::error().with_cause(RetentionFixture::error()));
    for error in errors {
        let expected = crate::live_failures::CaseFailure::from(error.clone()).evidence();
        let mut output = GeminiOutputRetention::from_metadata(
            RetentionFixture::JOB,
            &RetentionFixture::metadata(),
        )
        .unwrap();
        let rejected = output.defer(error).unwrap_err();
        assert!(!output.is_deferred());
        assert_eq!(
            crate::live_failures::CaseFailure::from(rejected).evidence(),
            expected
        );
    }
}

#[test]
fn gemini_output_retention_rejects_additional_structured_error_evidence() {
    for detail in [
        NativeDetail::Reason {
            domain: "googleapis.com".into(),
            reason: "BILLING_DISABLED".into(),
        },
        NativeDetail::QuotaViolation {
            subject: Some("project".into()),
            description: Some("quota exhausted".into()),
        },
        NativeDetail::Resource {
            resource_type: Some("file".into()),
            name: "files/different-output".into(),
            owner: None,
            description: None,
        },
        NativeDetail::Resource {
            resource_type: Some("model".into()),
            name: RetentionFixture::FILE.into(),
            owner: None,
            description: None,
        },
        NativeDetail::FieldViolation {
            field: "model".into(),
            reason: Some("INVALID_ARGUMENT".into()),
            description: Some("unrelated model validation".into()),
        },
    ] {
        let error = RetentionFixture::error().with_native_details(NativeErrorDetails {
            details: vec![detail],
            ..RetentionFixture::error().native_details().clone()
        });
        let expected = crate::live_failures::CaseFailure::from(error.clone()).evidence();
        let mut output = GeminiOutputRetention::from_metadata(
            RetentionFixture::JOB,
            &RetentionFixture::metadata(),
        )
        .unwrap();
        let rejected = output
            .defer(error)
            .expect_err("matching length prose must not hide additional structured evidence");
        assert!(!output.is_deferred());
        assert_eq!(
            crate::live_failures::CaseFailure::from(rejected).evidence(),
            expected,
        );
    }
}

#[test]
fn gemini_output_retention_rejects_alternate_native_error_signals() {
    for details in [
        NativeErrorDetails {
            rpc_code: Some(7),
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails {
            code: Some("quota_exceeded".into()),
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails {
            error_type: Some("authentication_error".into()),
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails {
            outcome: Some("OUTCOME_FAILED".into()),
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails {
            parameter: Some("model".into()),
            ..RetentionFixture::error().native_details().clone()
        },
        NativeErrorDetails {
            execution_exit_code: Some(1),
            ..RetentionFixture::error().native_details().clone()
        },
    ] {
        let error = RetentionFixture::error().with_native_details(details);
        let expected = crate::live_failures::CaseFailure::from(error.clone()).evidence();
        let mut output = GeminiOutputRetention::from_metadata(
            RetentionFixture::JOB,
            &RetentionFixture::metadata(),
        )
        .unwrap();
        let rejected = output
            .defer(error)
            .expect_err("matching length prose must not hide an alternate error signal");
        assert!(!output.is_deferred());
        assert_eq!(
            crate::live_failures::CaseFailure::from(rejected).evidence(),
            expected,
        );
    }
}

struct DeletedFixture;

impl DeletedFixture {
    fn submission_failure(error: provider_sdk::SdkError, definite: bool) {
        for input_known in [false, true] {
            let mut source = Self::uploaded_file(&Self::upload_request());
            source.id = "file-owned-source".into();
            source.input_reference = Some(FileReference::Provider {
                id: source.id.clone(),
                media_type: Some("text/plain".into()),
            });
            let mut publication = Self::source_publication(&source);
            let mut files = BTreeMap::from([
                (source.id.clone(), None),
                ("file-unrelated".to_owned(), None),
            ]);
            publication.bind_source(&source, 0, &mut files).unwrap();
            publication.upload_started().unwrap();
            if input_known {
                publication.uploaded_input("file-owned-input").unwrap();
                files.insert("file-owned-input".into(), Some(0));
            }
            publication.begin().unwrap();
            let before_files = files.clone();
            let expected_error = crate::live_failures::CaseFailure::from(error.clone()).evidence();
            let failure = publication.submission_failed(error.clone());
            assert_eq!(
                failure.evidence(),
                expected_error,
                "preserve the original SDK error"
            );
            assert_eq!(
                files, before_files,
                "classification must not change owned identities"
            );
            assert_eq!(publication.job_id(), None);
            assert_eq!(
                publication.input_id(),
                input_known.then_some("file-owned-input")
            );
            assert_eq!(publication.needs_input_discovery(), !input_known);
            assert_eq!(publication.publication_uncertain(), !definite);
            for unrelated_terminal in [false, true] {
                assert_eq!(
                    publication.can_delete_input(unrelated_terminal),
                    definite,
                    "a terminal flag cannot release an uncertain input or its bound source"
                );
            }
        }
    }

    fn submission_http(status: u16, phase: ErrorPhase) -> provider_sdk::SdkError {
        provider_sdk::SdkError::new(ErrorKind::InvalidRequest, Operation::SubmitBatch, phase)
            .with_http_status(status)
            .with_request_target("batches")
            .with_native_details(NativeErrorDetails {
                code: Some(format!("fixture-status-{status}")),
                message: Some("original batch outcome".into()),
                details: vec![NativeDetail::Resource {
                    resource_type: None,
                    name: "file-owned-input".into(),
                    owner: None,
                    description: None,
                }],
                ..Default::default()
            })
    }

    fn source_publication(source: &FileObject) -> InlinePublication {
        let reference = source
            .input_reference
            .clone()
            .expect("fixture input reference");
        let evidence = DeletedFileEvidence::new(source.id.clone(), reference);
        InlinePublication::new(
            "fixture-model",
            &[BatchInput::new("readability-row", evidence.read_request().unwrap()).unwrap()],
        )
        .unwrap()
    }

    fn upload_request() -> FileUploadRequest {
        FileUploadRequest::new(
            "fixture-owned.txt",
            "text/plain",
            "marker-café".as_bytes().to_vec(),
        )
        .unwrap()
    }

    fn uploaded_file(request: &FileUploadRequest) -> FileObject {
        FileObject {
            id: "files/owned".to_owned(),
            file_name: Some(request.file_name().to_owned()),
            size_bytes: Some(request.data().len() as u64),
            media_type: Some(request.media_type().to_owned()),
            created_at: None,
            state: Some(FileState::Active),
            input_reference: Some(FileReference::Url {
                url: "https://generativelanguage.googleapis.com/v1beta/files/owned"
                    .parse()
                    .unwrap(),
                media_type: Some(request.media_type().to_owned()),
            }),
        }
    }

    fn publication(id: &str, prompt: &str) -> InlinePublication {
        let request = GenerateRequest::new(vec![Message::user_text(prompt).unwrap()])
            .unwrap()
            .with_generation(
                GenerationConfig::default()
                    .with_max_output_tokens(std::num::NonZeroU32::new(4096).unwrap()),
            )
            .unwrap();
        InlinePublication::new("fixture-model", &[BatchInput::new(id, request).unwrap()]).unwrap()
    }

    fn native_row(id: &str, prompt: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"custom_id":id,"method":"POST","url":"/v1/responses","body":{
            "model":"fixture-model","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":prompt}]}],
            "max_output_tokens":4096,"store":false,"include":["reasoning.encrypted_content"]
        }})).unwrap()
    }
    fn evidence() -> DeletedFileEvidence {
        DeletedFileEvidence::new(
            "file-owned".to_owned(),
            FileReference::Provider {
                id: "file-owned".to_owned(),
                media_type: Some("text/plain".to_owned()),
            },
        )
    }

    fn page(ids: &[&str], cursor: Option<&str>) -> FilePage {
        FilePage {
            files: ids
                .iter()
                .map(|id| FileObject {
                    id: (*id).to_owned(),
                    file_name: None,
                    size_bytes: None,
                    media_type: None,
                    created_at: None,
                    state: None,
                    input_reference: None,
                })
                .collect(),
            next_cursor: cursor.map(str::to_owned),
        }
    }

    fn inventory(id: &str, present: bool) -> FileInventoryEvidence {
        let mut inventory = FileInventoryEvidence::new(id);
        let ids = if present { vec![id] } else { Vec::new() };
        inventory.observe(&Self::page(&ids, None)).unwrap();
        inventory
    }

    fn absent(mut evidence: DeletedFileEvidence) -> DeletedFileEvidence {
        evidence
            .preflight(&Fixture::response("Answer: file-marker"), "file-marker")
            .unwrap();
        Self::batch_readable(&mut evidence);
        let visible = Self::inventory(evidence.id(), true);
        evidence.visible(&visible).unwrap();
        evidence.deletion_acknowledged().unwrap();
        let absent = Self::inventory(evidence.id(), false);
        assert!(evidence.absent(&absent).unwrap());
        evidence
    }

    fn rows(code: Option<&str>, message: &str) -> Vec<BatchResult> {
        vec![
            Self::failed("bad", code, message),
            Fixture::succeeded("good", "answer"),
        ]
    }

    fn failed(id: &str, code: Option<&str>, message: &str) -> BatchResult {
        BatchResult::Failed {
            custom_id: id.to_owned(),
            error: Self::error(code, message),
        }
    }

    fn error(code: Option<&str>, message: &str) -> RemoteFailure {
        let (kind, error_type, rpc_code, native_status) = match code {
            Some("not_found_error") => (ErrorKind::NotFound, code.map(str::to_owned), None, None),
            Some("invalid_request_error") => (
                ErrorKind::InvalidRequest,
                code.map(str::to_owned),
                None,
                None,
            ),
            Some("3") => (ErrorKind::InvalidRequest, None, Some(3), None),
            Some("5") => (ErrorKind::NotFound, None, Some(5), None),
            Some("7") => (ErrorKind::PermissionDenied, None, Some(7), None),
            Some("INVALID_ARGUMENT") => (
                ErrorKind::InvalidRequest,
                None,
                None,
                code.map(str::to_owned),
            ),
            Some("NOT_FOUND") => (ErrorKind::NotFound, None, None, code.map(str::to_owned)),
            Some("PERMISSION_DENIED") => (
                ErrorKind::PermissionDenied,
                None,
                None,
                code.map(str::to_owned),
            ),
            _ => (ErrorKind::ProviderRejected, None, None, None),
        };
        RemoteFailure::new(
            kind,
            NativeErrorDetails {
                code: code.map(str::to_owned),
                error_type,
                rpc_code,
                native_status,
                message: Some(message.to_owned()),
                ..NativeErrorDetails::default()
            },
        )
        .unwrap()
    }

    fn batch_readable(evidence: &mut DeletedFileEvidence) {
        evidence
            .batch_preflight(
                &Fixture::job(BatchStatus::Completed),
                "job-1",
                &[Fixture::succeeded("readable", "Answer: file-marker")],
                "readable",
                "file-marker",
            )
            .unwrap();
    }

    fn complete(evidence: DeletedFileEvidence) -> DeletedFileEvidence {
        let mut evidence = Self::absent(evidence);
        let rows = Self::rows(Some("not_found_error"), "File 'file-owned' not found.");
        evidence
            .mixed_rows(&rows, ("good", "answer"), "bad")
            .unwrap();
        evidence
    }

    fn gemini_permission_details() -> NativeErrorDetails {
        NativeErrorDetails {
            rpc_code: Some(7),
            message: Some("The caller does not have permission".into()),
            ..Default::default()
        }
    }

    fn gemini_permission_error(details: NativeErrorDetails) -> RemoteFailure {
        serde_json::from_value(serde_json::json!({
            "kind": "permission_denied",
            "basis": ClassificationBasis::RpcCode,
            "details": details,
        }))
        .unwrap()
    }
}

#[test]
fn gemini_anonymous_deleted_file_row_accepts_observed_rpc_after_complete_proof() {
    let mut evidence = DeletedFixture::absent(DeletedFileEvidence::new(
        "files/owned-upload".into(),
        FileReference::Url {
            url: "https://generativelanguage.googleapis.com/v1beta/files/owned-upload"
                .parse()
                .unwrap(),
            media_type: Some("text/plain".into()),
        },
    ));
    let rows = vec![
        Fixture::succeeded("good", "answer"),
        BatchResult::Failed {
            custom_id: "bad".into(),
            error: DeletedFixture::gemini_permission_error(
                DeletedFixture::gemini_permission_details(),
            ),
        },
    ];
    let error = evidence
        .mixed_rows(&rows, ("good", "answer"), "bad")
        .unwrap();
    assert_eq!(error.classification_basis(), ClassificationBasis::RpcCode);
    evidence.gemini(error).unwrap();
    assert!(evidence.openai(error).is_err());
    assert!(evidence.anthropic(error).is_err());
}

#[test]
fn gemini_anonymous_deleted_file_row_requires_every_prerequisite_and_control() {
    let error =
        DeletedFixture::gemini_permission_error(DeletedFixture::gemini_permission_details());
    let mut evidence = DeletedFixture::evidence();
    assert!(evidence.gemini(&error).is_err(), "uploaded only");
    evidence
        .preflight(&Fixture::response("Answer: file-marker"), "file-marker")
        .unwrap();
    assert!(evidence.gemini(&error).is_err(), "unary readable only");
    DeletedFixture::batch_readable(&mut evidence);
    assert!(evidence.gemini(&error).is_err(), "batch readable only");
    let visible = DeletedFixture::inventory(evidence.id(), true);
    evidence.visible(&visible).unwrap();
    assert!(evidence.gemini(&error).is_err(), "visible before deletion");
    evidence.deletion_acknowledged().unwrap();
    assert!(
        evidence.gemini(&error).is_err(),
        "deletion acknowledged only"
    );
    assert!(!evidence.absent(&visible).unwrap());
    assert!(evidence.gemini(&error).is_err(), "file still visible");
    let absent = DeletedFixture::inventory(evidence.id(), false);
    assert!(evidence.absent(&absent).unwrap());
    assert!(evidence.gemini(&error).is_err(), "no correlated control");
}

#[test]
fn gemini_anonymous_deleted_file_row_rejects_invalid_mixed_control_evidence() {
    let error =
        DeletedFixture::gemini_permission_error(DeletedFixture::gemini_permission_details());
    let failed = BatchResult::Failed {
        custom_id: "bad".into(),
        error: error.clone(),
    };
    for rows in [
        vec![failed.clone()],
        vec![Fixture::succeeded("good", "answer")],
        vec![Fixture::succeeded("good", "wrong"), failed.clone()],
        vec![Fixture::succeeded("unexpected", "answer"), failed.clone()],
        vec![Fixture::succeeded("bad", "answer"), failed.clone()],
        vec![
            Fixture::succeeded("good", "answer"),
            failed.clone(),
            failed.clone(),
        ],
        vec![
            Fixture::succeeded("good", "answer"),
            BatchResult::Failed {
                custom_id: "unexpected".into(),
                error: error.clone(),
            },
        ],
        vec![
            Fixture::succeeded("good", "answer"),
            Fixture::succeeded("bad", "answer"),
        ],
        vec![
            BatchResult::Failed {
                custom_id: "good".into(),
                error: error.clone(),
            },
            failed.clone(),
        ],
        vec![
            BatchResult::Cancelled {
                custom_id: "good".into(),
            },
            failed.clone(),
        ],
        vec![
            BatchResult::Expired {
                custom_id: "good".into(),
            },
            failed,
        ],
    ] {
        let mut evidence = DeletedFixture::absent(DeletedFixture::evidence());
        assert!(
            evidence
                .mixed_rows(&rows, ("good", "answer"), "bad")
                .is_err()
        );
        assert!(evidence.gemini(&error).is_err());
    }
}

#[test]
fn gemini_anonymous_deleted_file_row_rejects_other_classification_sources() {
    let mut errors = Vec::new();
    for kind in [
        ErrorKind::PermissionDenied,
        ErrorKind::Authentication,
        ErrorKind::QuotaExceeded,
        ErrorKind::ResourceExhausted,
        ErrorKind::DeadlineExceeded,
        ErrorKind::ServiceUnavailable,
        ErrorKind::InvalidRequest,
        ErrorKind::NotFound,
    ] {
        errors.push(RemoteFailure::new(kind, DeletedFixture::gemini_permission_details()).unwrap());
    }
    for (kind, basis, details) in [
        (
            "permission_denied",
            ClassificationBasis::HttpStatus { status: 403 },
            NativeErrorDetails {
                rpc_code: None,
                ..DeletedFixture::gemini_permission_details()
            },
        ),
        (
            "permission_denied",
            ClassificationBasis::CanonicalStatus,
            NativeErrorDetails {
                rpc_code: None,
                native_status: Some("PERMISSION_DENIED".into()),
                ..DeletedFixture::gemini_permission_details()
            },
        ),
        (
            "authentication",
            ClassificationBasis::RpcCode,
            NativeErrorDetails {
                rpc_code: Some(16),
                ..DeletedFixture::gemini_permission_details()
            },
        ),
        (
            "resource_exhausted",
            ClassificationBasis::RpcCode,
            NativeErrorDetails {
                rpc_code: Some(8),
                ..DeletedFixture::gemini_permission_details()
            },
        ),
    ] {
        errors.push(
            serde_json::from_value(serde_json::json!({
                "kind": kind, "basis": basis, "details": details,
            }))
            .unwrap(),
        );
    }
    for error in errors {
        let mut evidence = DeletedFixture::absent(DeletedFixture::evidence());
        let rows = vec![
            Fixture::succeeded("good", "answer"),
            BatchResult::Failed {
                custom_id: "bad".into(),
                error,
            },
        ];
        let failure = evidence
            .mixed_rows(&rows, ("good", "answer"), "bad")
            .unwrap();
        assert!(evidence.gemini(failure).is_err());
    }
    for kind in [ErrorKind::Transport, ErrorKind::Timeout] {
        assert!(RemoteFailure::new(kind, DeletedFixture::gemini_permission_details()).is_err());
    }
    assert!(
        serde_json::from_value::<RemoteFailure>(serde_json::json!({
            "kind": "permission_denied", "basis": ClassificationBasis::RpcCode,
            "details": NativeErrorDetails {
                rpc_code: Some(403),
                ..DeletedFixture::gemini_permission_details()
            },
        }))
        .is_err(),
        "HTTP 403 cannot masquerade as RPC 7"
    );
}

#[test]
fn gemini_anonymous_deleted_file_row_rejects_incomplete_or_altered_diagnostics() {
    for details in [
        NativeErrorDetails {
            truncated: true,
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            truncated: true,
            omitted_details: 1,
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            message: None,
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            message: Some(String::new()),
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            message: Some("Permission denied for model unrelated-model".into()),
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            message: Some("The caller does not have permission to use this billing account".into()),
            ..DeletedFixture::gemini_permission_details()
        },
    ] {
        let mut evidence = DeletedFixture::absent(DeletedFixture::evidence());
        let rows = vec![
            Fixture::succeeded("good", "answer"),
            BatchResult::Failed {
                custom_id: "bad".into(),
                error: DeletedFixture::gemini_permission_error(details),
            },
        ];
        let error = evidence
            .mixed_rows(&rows, ("good", "answer"), "bad")
            .unwrap();
        assert!(evidence.gemini(error).is_err());
    }
}

#[test]
fn gemini_anonymous_deleted_file_row_rejects_richer_or_conflicting_details() {
    for detail in [
        NativeDetail::Resource {
            resource_type: Some("model".into()),
            name: "unrelated-model".into(),
            owner: None,
            description: None,
        },
        NativeDetail::Resource {
            resource_type: None,
            name: "files/different-upload".into(),
            owner: None,
            description: None,
        },
        NativeDetail::Reason {
            domain: "example.invalid".into(),
            reason: "BILLING_DISABLED".into(),
        },
        NativeDetail::QuotaViolation {
            subject: None,
            description: None,
        },
        NativeDetail::FieldViolation {
            field: "model".into(),
            reason: None,
            description: None,
        },
    ] {
        let mut evidence = DeletedFixture::absent(DeletedFixture::evidence());
        let rows = vec![
            Fixture::succeeded("good", "answer"),
            BatchResult::Failed {
                custom_id: "bad".into(),
                error: DeletedFixture::gemini_permission_error(NativeErrorDetails {
                    details: vec![detail],
                    ..DeletedFixture::gemini_permission_details()
                }),
            },
        ];
        let error = evidence
            .mixed_rows(&rows, ("good", "answer"), "bad")
            .unwrap();
        assert!(evidence.gemini(error).is_err());
    }
}

#[test]
fn gemini_anonymous_deleted_file_row_rejects_alternate_native_signals() {
    for details in [
        NativeErrorDetails {
            native_status: Some("PERMISSION_DENIED".into()),
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            native_status: Some("UNAUTHENTICATED".into()),
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            code: Some("7".into()),
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            code: Some("quota_exceeded".into()),
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            error_type: Some("authentication_error".into()),
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            reported_http_status: Some(403),
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            parameter: Some("model".into()),
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            outcome: Some("URL_RETRIEVAL_STATUS_PAYWALL".into()),
            ..DeletedFixture::gemini_permission_details()
        },
        NativeErrorDetails {
            execution_exit_code: Some(1),
            ..DeletedFixture::gemini_permission_details()
        },
    ] {
        let mut evidence = DeletedFixture::absent(DeletedFixture::evidence());
        let rows = vec![
            Fixture::succeeded("good", "answer"),
            BatchResult::Failed {
                custom_id: "bad".into(),
                error: DeletedFixture::gemini_permission_error(details),
            },
        ];
        let error = evidence
            .mixed_rows(&rows, ("good", "answer"), "bad")
            .unwrap();
        assert!(evidence.gemini(error).is_err());
    }
}

#[test]
fn deleted_file_row_accepts_identified_validation_rejection() {
    let evidence = DeletedFixture::complete(DeletedFixture::evidence());
    assert!(
        evidence
            .anthropic(&DeletedFixture::error(
                Some("invalid_request_error"),
                "File 'file-owned' not found."
            ))
            .is_ok()
    );
}

#[test]
fn deleted_file_row_rejects_unrelated_resource_diagnostic() {
    let evidence = DeletedFixture::complete(DeletedFixture::evidence());
    assert!(
        evidence
            .anthropic(&DeletedFixture::error(
                Some("not_found_error"),
                "Model 'unrelated-model' not found."
            ))
            .is_err()
    );
}

#[test]
fn file_inventory_requires_all_pages_and_exact_owned_identity() {
    let mut inventory = FileInventoryEvidence::new("file-owned");
    assert!(inventory.contains("file-owned").is_err());
    assert_eq!(
        inventory
            .observe(&DeletedFixture::page(&[], Some("page-2")))
            .unwrap(),
        Some("page-2".to_owned())
    );
    assert!(inventory.contains("file-owned").is_err());
    inventory
        .observe(&DeletedFixture::page(&["other", "file-owned"], None))
        .unwrap();
    assert!(inventory.contains("file-owned").unwrap());
    assert!(inventory.contains("other").is_err());
    let mut absent = FileInventoryEvidence::new("file-owned");
    absent
        .observe(&DeletedFixture::page(&["file-owned-extra"], Some("more")))
        .unwrap();
    absent.observe(&DeletedFixture::page(&[], None)).unwrap();
    assert!(!absent.contains("file-owned").unwrap());
}

#[test]
fn file_inventory_rejects_empty_repeated_or_post_terminal_cursors() {
    for cursor in ["", " "] {
        let mut inventory = FileInventoryEvidence::new("file-owned");
        assert!(
            inventory
                .observe(&DeletedFixture::page(&[], Some(cursor)))
                .is_err()
        );
        assert!(inventory.contains("file-owned").is_err());
    }
    let mut inventory = FileInventoryEvidence::new("file-owned");
    inventory
        .observe(&DeletedFixture::page(&[], Some("same")))
        .unwrap();
    assert!(
        inventory
            .observe(&DeletedFixture::page(&[], Some("same")))
            .is_err()
    );
    assert!(inventory.observe(&DeletedFixture::page(&[], None)).is_err());
    assert!(inventory.contains("file-owned").is_err());
    let mut inventory = DeletedFixture::inventory("file-owned", false);
    assert!(inventory.observe(&DeletedFixture::page(&[], None)).is_err());
    assert!(inventory.contains("file-owned").is_err());
}

#[test]
fn file_inventory_listing_error_cannot_be_recovered_into_absence() {
    let mut inventory = FileInventoryEvidence::new("file-owned");
    inventory
        .observe_result(Ok(DeletedFixture::page(&[], Some("page-2"))))
        .unwrap();
    assert_eq!(
        inventory
            .observe_result(Err("listing failed".into()))
            .unwrap_err()
            .to_string(),
        "listing failed"
    );
    assert!(inventory.contains("file-owned").is_err());
    assert!(
        inventory
            .observe_result(Ok(DeletedFixture::page(&[], None)))
            .is_err()
    );
    assert!(inventory.contains("file-owned").is_err());
}

#[test]
fn deleted_file_proof_requires_preflight_visibility_deletion_absence_and_control() {
    let visible = DeletedFixture::inventory("file-owned", true);
    let absent = DeletedFixture::inventory("file-owned", false);
    let rows = DeletedFixture::rows(Some("not_found_error"), "file-owned");
    let mut evidence = DeletedFixture::evidence();
    assert!(evidence.visible(&visible).is_err());
    assert!(evidence.deletion_acknowledged().is_err());
    assert!(evidence.absent(&absent).is_err());
    assert!(
        evidence
            .mixed_rows(&rows, ("good", "answer"), "bad")
            .is_err()
    );
    assert!(
        evidence
            .anthropic(&DeletedFixture::error(
                Some("not_found_error"),
                "file-owned"
            ))
            .is_err()
    );
    assert!(
        evidence
            .preflight(&Fixture::response("wrong"), "file-marker")
            .is_err()
    );
    evidence
        .preflight(&Fixture::response("Answer: file-marker"), "file-marker")
        .unwrap();
    assert!(evidence.visible(&visible).is_err());
    DeletedFixture::batch_readable(&mut evidence);
    assert!(evidence.deletion_acknowledged().is_err());
    assert!(evidence.visible(&absent).is_err());
    evidence.visible(&visible).unwrap();
    assert!(evidence.absent(&absent).is_err());
    evidence.deletion_acknowledged().unwrap();
    assert!(!evidence.absent(&visible).unwrap());
    let incomplete = FileInventoryEvidence::new("file-owned");
    assert!(evidence.absent(&incomplete).is_err());
    assert!(evidence.absent(&absent).unwrap());
    assert!(
        evidence
            .anthropic(&DeletedFixture::error(
                Some("not_found_error"),
                "file-owned"
            ))
            .is_err()
    );
    evidence
        .mixed_rows(&rows, ("good", "answer"), "bad")
        .unwrap();
    assert!(
        evidence
            .anthropic(&DeletedFixture::error(
                Some("not_found_error"),
                "file-owned"
            ))
            .is_ok()
    );
}

#[test]
fn mixed_rows_reject_missing_duplicates_wrong_outcomes_and_wrong_control_answer() {
    let failures = vec![
        vec![Fixture::succeeded("good", "answer")],
        vec![
            Fixture::succeeded("good", "answer"),
            Fixture::succeeded("good", "answer"),
        ],
        vec![
            Fixture::succeeded("bad", "answer"),
            DeletedFixture::failed("good", Some("not_found_error"), "file-owned"),
        ],
        vec![
            Fixture::succeeded("good", "wrong"),
            DeletedFixture::failed("bad", Some("not_found_error"), "file-owned"),
        ],
        vec![
            Fixture::succeeded("good", "answer"),
            BatchResult::Cancelled {
                custom_id: "bad".into(),
            },
        ],
        vec![
            Fixture::succeeded("good", "answer"),
            BatchResult::Expired {
                custom_id: "bad".into(),
            },
        ],
        vec![
            DeletedFixture::failed("good", Some("not_found_error"), "file-owned"),
            DeletedFixture::failed("bad", Some("not_found_error"), "file-owned"),
        ],
        vec![
            Fixture::succeeded("good", "answer"),
            DeletedFixture::failed("unexpected", Some("not_found_error"), "file-owned"),
        ],
    ];
    for rows in failures {
        let mut evidence = DeletedFixture::absent(DeletedFixture::evidence());
        assert!(
            evidence
                .mixed_rows(&rows, ("good", "answer"), "bad")
                .is_err()
        );
        assert!(
            evidence
                .anthropic(&DeletedFixture::error(
                    Some("not_found_error"),
                    "file-owned"
                ))
                .is_err()
        );
    }
}

#[test]
fn deleted_file_codes_are_closed_and_rpc_numbers_are_not_http_statuses() {
    let evidence = DeletedFixture::complete(DeletedFixture::evidence());
    for code in [
        "file_not_found",
        "invalid_file",
        "invalid_file_id",
        "invalid_request_error",
    ] {
        assert!(
            evidence
                .openai(&DeletedFixture::error(Some(code), "file-owned"))
                .is_ok()
        );
    }
    for code in ["not_found_error", "invalid_request_error"] {
        assert!(
            evidence
                .anthropic(&DeletedFixture::error(Some(code), "file-owned"))
                .is_ok()
        );
    }
    for code in [
        "NOT_FOUND",
        "5",
        "INVALID_ARGUMENT",
        "3",
        "PERMISSION_DENIED",
        "7",
    ] {
        assert!(
            evidence
                .gemini(&DeletedFixture::error(Some(code), "file-owned"))
                .is_ok()
        );
    }
    for code in [
        None,
        Some(""),
        Some("error"),
        Some("authentication_error"),
        Some("insufficient_quota"),
        Some("rate_limit_error"),
        Some("INTERNAL"),
        Some("DEADLINE_EXCEEDED"),
        Some("400"),
        Some("403"),
        Some("404"),
        Some("429"),
        Some("RESOURCE_EXHAUSTED"),
        Some("8"),
        Some("16"),
        Some("UNAUTHENTICATED"),
        Some("unknown"),
    ] {
        assert!(
            evidence
                .openai(&DeletedFixture::error(code, "file-owned"))
                .is_err()
        );
        assert!(
            evidence
                .anthropic(&DeletedFixture::error(code, "file-owned"))
                .is_err()
        );
        assert!(
            evidence
                .gemini(&DeletedFixture::error(code, "file-owned"))
                .is_err()
        );
    }
    assert!(
        evidence
            .gemini(&DeletedFixture::error(
                Some("PERMISSION_DENIED"),
                "Permission denied for this project"
            ))
            .is_err()
    );
    assert!(
        evidence
            .gemini(&DeletedFixture::error(
                Some("INVALID_ARGUMENT"),
                "Malformed generation request"
            ))
            .is_err()
    );
}

#[test]
fn deleted_file_identity_requires_a_complete_exact_id_or_original_uri() {
    let evidence = DeletedFixture::complete(DeletedFixture::evidence());
    for message in [
        "file-owned",
        "File 'file-owned' not found.",
        "File `file-owned` not found.",
        "file-owned.",
        "(file-owned)",
    ] {
        assert!(
            evidence
                .anthropic(&DeletedFixture::error(Some("not_found_error"), message))
                .is_ok()
        );
    }
    for message in [
        "",
        "File 'FILE-owned' not found",
        "prefix-file-owned",
        "file-owned-extra",
        "file-owned.extension",
        "https://unrelated.example/file-owned",
        "file-owned?other",
        "file-owned/other",
    ] {
        assert!(
            evidence
                .anthropic(&DeletedFixture::error(Some("not_found_error"), message))
                .is_err()
        );
    }
    let uri = "https://generativelanguage.googleapis.com/v1beta/files/owned-token";
    let original = FileReference::Url {
        url: uri.parse().unwrap(),
        media_type: Some("text/plain".into()),
    };
    let evidence = DeletedFixture::complete(DeletedFileEvidence::new(
        "files/owned-token".into(),
        original.clone(),
    ));
    assert_eq!(evidence.id(), "files/owned-token");
    assert!(
        matches!(&evidence.read_request().unwrap().messages()[0].content()[1], provider_sdk::generation::ContentPart::File { reference } if reference == &original)
    );
    for identity in ["files/owned-token", uri] {
        assert!(
            evidence
                .gemini(&DeletedFixture::error(
                    Some("7"),
                    &format!("Cannot access file '{identity}'")
                ))
                .is_ok()
        );
    }
    for identity in [
        "owned-token",
        "files/owned-token-extra",
        "https://elsewhere.example/v1beta/files/owned-token",
        "https://generativelanguage.googleapis.com/v1beta/files/owned-token?other",
    ] {
        assert!(
            evidence
                .gemini(&DeletedFixture::error(
                    Some("7"),
                    &format!("Cannot access file '{identity}'")
                ))
                .is_err()
        );
    }
}

#[test]
fn batch_deletion_requires_terminal_ownership_and_prior_artifact_capture() {
    let mut cleanup = BatchCleanup::new();
    assert!(cleanup.needs_status());
    assert!(cleanup.artifacts_captured().is_err());
    assert!(cleanup.begin_delete().is_err());
    assert!(cleanup.deletion_acknowledged(b"").is_err());
    assert!(
        cleanup
            .terminal(&Fixture::job(BatchStatus::Queued), "job-1")
            .is_err()
    );
    assert!(
        cleanup
            .terminal(&Fixture::job(BatchStatus::Running), "job-1")
            .is_err()
    );
    assert!(
        cleanup
            .terminal(&Fixture::job(BatchStatus::Completed), "different-job")
            .is_err()
    );
    cleanup
        .terminal(&Fixture::job(BatchStatus::Completed), "job-1")
        .unwrap();
    assert!(!cleanup.needs_status());
    assert!(cleanup.needs_artifacts());
    assert!(cleanup.begin_delete().is_err());
    cleanup.artifacts_captured().unwrap();
    assert!(!cleanup.needs_artifacts());
    cleanup.begin_delete().unwrap();
    assert!(cleanup.deletion_uncertain());
    assert!(!cleanup.needs_status());
    assert!(!cleanup.needs_artifacts());
    assert!(
        cleanup.begin_delete().is_err(),
        "a second native deletion must not be admitted"
    );
    cleanup.deletion_acknowledged(b"").unwrap();
    assert!(!cleanup.deletion_uncertain());
    assert!(!cleanup.needs_status());
    assert!(!cleanup.needs_artifacts());
    assert!(cleanup.begin_delete().is_err());
    assert!(
        cleanup
            .terminal(&Fixture::job(BatchStatus::Completed), "job-1")
            .is_err()
    );
}

#[test]
fn batch_deletion_fixture_accepts_every_owned_terminal_state() {
    for status in [
        BatchStatus::Completed,
        BatchStatus::Failed {
            errors: vec![DeletedFixture::error(
                Some("fixture-failed"),
                "owned terminal fixture",
            )],
        },
        BatchStatus::Cancelled,
        BatchStatus::Expired,
    ] {
        let mut cleanup = BatchCleanup::new();
        cleanup.terminal(&Fixture::job(status), "job-1").unwrap();
        assert!(!cleanup.needs_status());
        assert!(cleanup.needs_artifacts());
        cleanup.artifacts_captured().unwrap();
        cleanup.begin_delete().unwrap();
        cleanup.deletion_acknowledged(b"{}").unwrap();
        assert!(!cleanup.deletion_uncertain());
    }
}

#[test]
fn batch_deletion_unexpected_response_preserves_uncertainty_without_retry() {
    for body in [
        b"[]".as_slice(),
        b"null",
        b"{\"deleted\":true}",
        b"invalid",
        b" ",
    ] {
        let mut cleanup = BatchCleanup::new();
        cleanup
            .terminal(&Fixture::job(BatchStatus::Completed), "job-1")
            .unwrap();
        cleanup.artifacts_captured().unwrap();
        cleanup.begin_delete().unwrap();
        assert!(cleanup.deletion_acknowledged(body).is_err());
        assert!(cleanup.deletion_uncertain());
        assert!(!cleanup.needs_status());
        assert!(!cleanup.needs_artifacts());
        assert!(cleanup.begin_delete().is_err());
    }
}

#[test]
fn deleted_file_readability_uses_a_single_answer_field() {
    let mut evidence = DeletedFixture::evidence();
    evidence.preflight(
        &Fixture::response("The attached file contains this value.\nAnswer: independent-secret.\nRead from the attachment."),
        "independent-secret",
    ).unwrap();
}

#[test]
fn unary_file_readability_alone_cannot_authorize_deletion() {
    let mut evidence = DeletedFixture::evidence();
    evidence
        .preflight(&Fixture::response("Answer: file-marker"), "file-marker")
        .unwrap();
    assert!(
        evidence
            .visible(&DeletedFixture::inventory("file-owned", true))
            .is_err(),
        "the identical file request must also succeed through batch mode before deletion"
    );
}

#[test]
fn deleted_file_row_uses_anthropic_error_type_in_its_native_namespace() {
    let evidence = DeletedFixture::complete(DeletedFixture::evidence());
    for (kind, error_type) in [
        (ErrorKind::NotFound, "not_found_error"),
        (ErrorKind::InvalidRequest, "invalid_request_error"),
    ] {
        let error = RemoteFailure::new(
            kind,
            NativeErrorDetails {
                error_type: Some(error_type.into()),
                message: Some("File 'file-owned' was not found".into()),
                ..Default::default()
            },
        )
        .unwrap();
        evidence.anthropic(&error).unwrap();
    }
}

#[test]
fn deleted_file_row_uses_rpc_codes_and_canonical_status_in_their_native_namespaces() {
    let evidence = DeletedFixture::complete(DeletedFixture::evidence());
    for (kind, rpc, status) in [
        (ErrorKind::InvalidRequest, 3, "INVALID_ARGUMENT"),
        (ErrorKind::NotFound, 5, "NOT_FOUND"),
        (ErrorKind::PermissionDenied, 7, "PERMISSION_DENIED"),
    ] {
        for (rpc_code, native_status) in [(Some(rpc), None), (None, Some(status.into()))] {
            let error = RemoteFailure::new(
                kind,
                NativeErrorDetails {
                    rpc_code,
                    native_status,
                    message: Some("Cannot read file 'file-owned'".into()),
                    ..Default::default()
                },
            )
            .unwrap();
            evidence.gemini(&error).unwrap();
        }
    }
}

#[test]
fn deleted_file_rejection_cannot_be_proven_by_a_conflicting_authentication_category() {
    let evidence = DeletedFixture::complete(DeletedFixture::evidence());
    let error = RemoteFailure::new(
        ErrorKind::Authentication,
        NativeErrorDetails {
            code: Some("invalid_file".into()),
            message: Some("file-owned".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(evidence.openai(&error).is_err());
}

#[test]
fn deleted_file_diagnostic_accepts_exact_native_resource_identity() {
    let evidence = DeletedFixture::complete(DeletedFixture::evidence());
    let error = RemoteFailure::new(
        ErrorKind::NotFound,
        NativeErrorDetails {
            rpc_code: Some(5),
            details: vec![NativeDetail::Resource {
                resource_type: Some("generativelanguage.googleapis.com/File".into()),
                name: "file-owned".into(),
                owner: None,
                description: None,
            }],
            ..Default::default()
        },
    )
    .unwrap();
    evidence.gemini(&error).unwrap();
}

#[test]
fn inline_input_ownership_is_exact_and_specific_to_one_submission() {
    let first = DeletedFixture::publication("first-owned", "first immutable prompt");
    let second = DeletedFixture::publication("second-owned", "second immutable prompt");
    let first_bytes = DeletedFixture::native_row("first-owned", "first immutable prompt");
    let second_bytes = DeletedFixture::native_row("second-owned", "second immutable prompt");
    assert!(first.owns_input(&first_bytes));
    assert!(!first.owns_input(&second_bytes));
    assert!(second.owns_input(&second_bytes));
    let mut combined = first_bytes.clone();
    combined.push(b'\n');
    combined.extend(&second_bytes);
    assert!(!first.owns_input(&combined));
    assert!(!second.owns_input(&combined));
    for (pointer, value) in [
        (
            "/body/input/0/content/0/text",
            serde_json::json!("different prompt"),
        ),
        ("/body/model", serde_json::json!("different-model")),
        ("/body/max_output_tokens", serde_json::json!(8192)),
        ("/body/store", serde_json::json!(true)),
        ("/body/include", serde_json::json!([])),
        ("/method", serde_json::json!("GET")),
        ("/url", serde_json::json!("/v1/chat/completions")),
    ] {
        let mut row: serde_json::Value = serde_json::from_slice(&first_bytes).unwrap();
        *row.pointer_mut(pointer).unwrap() = value;
        assert!(!first.owns_input(&serde_json::to_vec(&row).unwrap()));
    }
    let mut row: serde_json::Value = serde_json::from_slice(&first_bytes).unwrap();
    row["body"]["extra"] = serde_json::json!(true);
    assert!(!first.owns_input(&serde_json::to_vec(&row).unwrap()));
    let mut duplicate = first_bytes.clone();
    duplicate.push(b'\n');
    duplicate.extend(first_bytes);
    assert!(!first.owns_input(&duplicate));
}

#[test]
fn publication_uncertainty_and_upload_discovery_are_independent_obligations() {
    let mut first = DeletedFixture::publication("first-owned", "first prompt");
    let mut second = DeletedFixture::publication("second-owned", "second prompt");
    first.upload_started().unwrap();
    second.upload_started().unwrap();
    first.begin().unwrap();
    second.begin().unwrap();
    assert!(first.publication_uncertain());
    assert!(first.needs_input_discovery());
    assert!(!first.can_delete_input(true));
    second.accepted("job-second").unwrap();
    second.capture_input("job-second", "file-second").unwrap();
    assert!(!second.needs_input_discovery());
    assert!(first.needs_input_discovery());
    assert!(!second.can_delete_input(false));
    assert!(second.can_delete_input(true));
    assert!(second.capture_input("job-first", "file-first").is_err());
    assert!(second.begin().is_err());
    first.rejected().unwrap();
    assert!(!first.publication_uncertain());
    assert!(
        first.needs_input_discovery(),
        "a rejected job may still have an uploaded input artifact"
    );
    first
        .discover_input(
            "file-first",
            &DeletedFixture::native_row("first-owned", "first prompt"),
        )
        .unwrap();
    assert_eq!(first.input_id(), Some("file-first"));
    assert!(first.can_delete_input(false));
    assert!(
        first.begin().is_err(),
        "failed scenarios must not be replayed"
    );
}

#[test]
fn ambiguous_publication_keeps_its_exact_discovered_input_protected() {
    let mut publication = DeletedFixture::publication("owned", "immutable prompt");
    publication.upload_started().unwrap();
    publication.begin().unwrap();
    assert!(publication.accepted("").is_err());
    assert!(publication.publication_uncertain());
    publication
        .discover_input(
            "file-owned",
            &DeletedFixture::native_row("owned", "immutable prompt"),
        )
        .unwrap();
    assert!(publication.publication_uncertain());
    assert!(!publication.can_delete_input(true));
    assert!(!publication.needs_input_discovery());
}

#[test]
fn batch_file_readability_requires_successful_exact_job_and_row_before_visibility() {
    for (job, rows, row_id) in [
        (
            Fixture::job(BatchStatus::Failed { errors: Vec::new() }),
            vec![Fixture::succeeded("readable", "Answer: file-marker")],
            "readable",
        ),
        (
            Fixture::job(BatchStatus::Completed),
            vec![Fixture::succeeded("different", "Answer: file-marker")],
            "readable",
        ),
        (
            Fixture::job(BatchStatus::Completed),
            vec![Fixture::succeeded("readable", "Answer: wrong")],
            "readable",
        ),
        (
            Fixture::job(BatchStatus::Completed),
            vec![
                Fixture::succeeded("readable", "Answer: file-marker"),
                Fixture::succeeded("readable", "Answer: file-marker"),
            ],
            "readable",
        ),
        (
            Fixture::job(BatchStatus::Completed),
            vec![DeletedFixture::failed(
                "readable",
                Some("invalid_file"),
                "file-owned",
            )],
            "readable",
        ),
    ] {
        let mut evidence = DeletedFixture::evidence();
        evidence
            .preflight(&Fixture::response("Answer: file-marker"), "file-marker")
            .unwrap();
        assert!(
            evidence
                .batch_preflight(&job, "job-1", &rows, row_id, "file-marker")
                .is_err()
        );
        assert!(
            evidence
                .visible(&DeletedFixture::inventory("file-owned", true))
                .is_err()
        );
    }
}

#[test]
fn attached_file_request_never_contains_the_independent_secret_and_preserves_reference() {
    let evidence = DeletedFixture::evidence();
    let secret = "independently-random-file-secret";
    let request = evidence.read_request().unwrap();
    let body = serde_json::to_string(&request).unwrap();
    assert!(!body.contains(secret));
    let parts = request.messages()[0].content();
    assert!(
        matches!(&parts[1], provider_sdk::generation::ContentPart::File { reference: FileReference::Provider { id, media_type } } if id == "file-owned" && media_type.as_deref() == Some("text/plain"))
    );
    let row = BatchInput::new("preflight-row", request.clone()).unwrap();
    let failure_row = BatchInput::new("deleted-row", request.clone()).unwrap();
    assert_eq!(row.request(), &request);
    assert_eq!(failure_row.request(), &request);
}

#[test]
fn native_prepared_upload_is_owned_before_submission_and_never_replayed() {
    let bytes = DeletedFixture::native_row("prepared", "native fixture prompt");
    let mut publication = InlinePublication::from_prepared_jsonl("fixture-model", &bytes).unwrap();
    publication.upload_started().unwrap();
    assert!(publication.needs_input_discovery());
    assert!(!publication.publication_uncertain());
    assert!(
        publication.can_delete_input(false),
        "no job was submitted during native file setup"
    );
    assert!(publication.upload_started().is_err());
    publication.uploaded_input("file-native").unwrap();
    assert_eq!(publication.input_id(), Some("file-native"));
    publication.begin().unwrap();
    publication.accepted("native-job").unwrap();
    assert_eq!(publication.job_id(), Some("native-job"));
    assert!(publication.uploaded_input("file-other").is_err());
    assert!(InlinePublication::from_prepared_jsonl("wrong-model", &bytes).is_err());
}

#[test]
fn inline_input_ownership_checks_the_original_file_reference() {
    let evidence = DeletedFixture::evidence();
    let row = BatchInput::new("file-row", evidence.read_request().unwrap()).unwrap();
    let publication = InlinePublication::new("fixture-model", &[row]).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&DeletedFixture::native_row("file-row",
        "What is the confirmation code in the attached delivery confirmation record? Return one line in the form Answer: <confirmation code>. Do not decorate that field; put any explanation on other lines.")).unwrap();
    value["body"]["input"][0]["content"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"type":"input_file","file_id":"file-owned"}));
    assert!(publication.owns_input(&serde_json::to_vec(&value).unwrap()));
    value["body"]["input"][0]["content"][1]["file_id"] = serde_json::json!("file-other");
    assert!(!publication.owns_input(&serde_json::to_vec(&value).unwrap()));
}

#[test]
fn deleted_file_causal_chain_does_not_accept_truncated_or_conflicting_native_evidence() {
    let evidence = DeletedFixture::complete(DeletedFixture::evidence());
    for details in [
        NativeErrorDetails {
            rpc_code: Some(5),
            native_status: Some("PERMISSION_DENIED".into()),
            message: Some("file-owned".into()),
            ..Default::default()
        },
        NativeErrorDetails {
            rpc_code: Some(5),
            truncated: true,
            message: Some("file-owned".into()),
            ..Default::default()
        },
        NativeErrorDetails {
            rpc_code: Some(8),
            message: Some("file-owned".into()),
            ..Default::default()
        },
        NativeErrorDetails {
            code: Some("5".into()),
            message: Some("file-owned".into()),
            ..Default::default()
        },
    ] {
        assert!(
            evidence
                .gemini(&RemoteFailure::new(ErrorKind::NotFound, details).unwrap())
                .is_err()
        );
    }
}

#[test]
fn deleted_file_proof_does_not_override_authoritative_wrong_resource_with_prose() {
    let evidence = DeletedFixture::complete(DeletedFixture::evidence());
    for names in [vec!["different-file"], vec!["file-owned", "different-file"]] {
        let error = RemoteFailure::new(
            ErrorKind::NotFound,
            NativeErrorDetails {
                rpc_code: Some(5),
                message: Some("Cannot read 'file-owned'".into()),
                details: names
                    .into_iter()
                    .map(|name| NativeDetail::Resource {
                        resource_type: None,
                        name: name.into(),
                        owner: None,
                        description: None,
                    })
                    .collect(),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(evidence.gemini(&error).is_err());
    }
}

#[test]
fn publication_without_an_input_upload_does_not_invent_a_file_cleanup_obligation() {
    let mut publication = DeletedFixture::publication("inline-native", "ordinary inline request");
    publication.begin().unwrap();
    assert!(publication.publication_uncertain());
    assert!(
        !publication.needs_input_discovery(),
        "Gemini and Anthropic inline requests publish no input-file artifact"
    );
    publication.accepted("native-inline-job").unwrap();
    assert!(!publication.needs_input_discovery());
}

#[test]
fn source_binding_retains_exact_prerequisite_file_until_its_job_is_terminal() {
    for provider_reference in [false, true] {
        let mut source = DeletedFixture::uploaded_file(&DeletedFixture::upload_request());
        if provider_reference {
            source.id = "file-source".into();
            source.input_reference = Some(FileReference::Provider {
                id: source.id.clone(),
                media_type: Some("text/plain".into()),
            });
        }
        let mut publication = DeletedFixture::source_publication(&source);
        let mut files = BTreeMap::from([
            (source.id.clone(), None),
            ("unrelated-owned-file".to_owned(), None),
        ]);
        publication.bind_source(&source, 0, &mut files).unwrap();
        assert_eq!(files.get(&source.id), Some(&Some(0)));
        assert_eq!(files.get("unrelated-owned-file"), Some(&None));
        assert!(
            publication.input_id().is_none(),
            "the ordinary source is not the JSONL input"
        );
        assert!(
            publication.can_delete_input(false),
            "unsubmitted prepared work has no dependency"
        );
        publication.begin().unwrap();
        assert!(!publication.can_delete_input(false));
        assert!(
            !publication.can_delete_input(true),
            "unknown publication cannot be made terminal by an unrelated job"
        );
        assert_eq!(files.get(&source.id), Some(&Some(0)));
        publication.accepted("job-1").unwrap();
        publication
            .capture_input("job-1", "file-jsonl-input")
            .unwrap();
        assert_eq!(publication.input_id(), Some("file-jsonl-input"));
        assert_ne!(publication.input_id(), Some(source.id.as_str()));
        let mut cleanup = BatchCleanup::new();
        assert!(!publication.can_delete_input(!cleanup.needs_status()));
        let mut other = Fixture::job(BatchStatus::Completed);
        other.id = "unrelated-job".into();
        assert!(cleanup.terminal(&other, "job-1").is_err());
        assert!(!publication.can_delete_input(!cleanup.needs_status()));
        cleanup
            .terminal(&Fixture::job(BatchStatus::Completed), "job-1")
            .unwrap();
        assert!(publication.can_delete_input(!cleanup.needs_status()));
        assert_eq!(files.get(&source.id), Some(&Some(0)));
    }
}

#[test]
fn source_binding_definitive_rejection_releases_source_without_a_job() {
    let source = DeletedFixture::uploaded_file(&DeletedFixture::upload_request());
    let mut publication = DeletedFixture::source_publication(&source);
    let mut files = BTreeMap::from([(source.id.clone(), None)]);
    publication.bind_source(&source, 0, &mut files).unwrap();
    publication.begin().unwrap();
    assert!(!publication.can_delete_input(false));
    publication.rejected().unwrap();
    assert!(publication.can_delete_input(false));
    assert!(publication.job_id().is_none());
    assert_eq!(files.get(&source.id), Some(&Some(0)));
}

#[test]
fn batch_submission_cleanup_keeps_http_408_uncertain() {
    DeletedFixture::submission_failure(
        DeletedFixture::submission_http(408, ErrorPhase::ResponseHeaders),
        false,
    );
}

#[test]
fn batch_submission_cleanup_keeps_http_409_uncertain() {
    DeletedFixture::submission_failure(
        DeletedFixture::submission_http(409, ErrorPhase::ResponseHeaders),
        false,
    );
}

#[test]
fn batch_submission_cleanup_does_not_infer_rejection_from_auth_or_quota_status() {
    for status in [401, 403, 429] {
        DeletedFixture::submission_failure(
            DeletedFixture::submission_http(status, ErrorPhase::ResponseHeaders),
            false,
        );
    }
}

#[test]
fn batch_submission_cleanup_releases_definite_response_rejections() {
    for status in [400, 422] {
        DeletedFixture::submission_failure(
            DeletedFixture::submission_http(status, ErrorPhase::ResponseHeaders),
            true,
        );
    }
}

#[test]
fn batch_submission_cleanup_requires_response_header_rejection_evidence() {
    for phase in [
        ErrorPhase::RequestSend,
        ErrorPhase::ReadBody,
        ErrorPhase::Decode,
    ] {
        for status in [400, 422] {
            DeletedFixture::submission_failure(
                DeletedFixture::submission_http(status, phase),
                false,
            );
        }
    }
}

#[test]
fn batch_submission_cleanup_releases_local_request_build_failure() {
    for (kind, phase) in [
        (ErrorKind::InvalidInput, ErrorPhase::RequestBuild),
        (ErrorKind::Configuration, ErrorPhase::Configuration),
        (ErrorKind::InvalidInput, ErrorPhase::InputValidation),
        (ErrorKind::Unsupported, ErrorPhase::InputValidation),
        (ErrorKind::Closed, ErrorPhase::Lifecycle),
    ] {
        DeletedFixture::submission_failure(
            provider_sdk::SdkError::new(kind, Operation::SubmitBatch, phase),
            true,
        );
    }
}

#[test]
fn batch_submission_cleanup_keeps_transport_and_malformed_success_uncertain() {
    for error in [
        provider_sdk::SdkError::new(
            ErrorKind::Transport,
            Operation::SubmitBatch,
            ErrorPhase::RequestSend,
        ),
        provider_sdk::SdkError::new(
            ErrorKind::InvalidResponse(ResponseViolation::MalformedJson),
            Operation::SubmitBatch,
            ErrorPhase::Decode,
        )
        .with_http_status(200),
    ] {
        DeletedFixture::submission_failure(error, false);
    }
}

#[test]
fn source_binding_rejects_unknown_or_reassigned_sources_atomically() {
    let source = DeletedFixture::uploaded_file(&DeletedFixture::upload_request());
    let publication = DeletedFixture::source_publication(&source);
    for mut files in [
        BTreeMap::new(),
        BTreeMap::from([(source.id.clone(), Some(3))]),
        BTreeMap::from([(source.id.clone(), Some(0))]),
    ] {
        let before = files.clone();
        assert!(publication.bind_source(&source, 0, &mut files).is_err());
        assert_eq!(files, before, "invalid binding must not mutate ownership");
    }
    let mut files = BTreeMap::from([(source.id.clone(), None)]);
    let before = files.clone();
    let text_only = DeletedFixture::publication("unrelated-row", "no attached source");
    assert!(text_only.bind_source(&source, 0, &mut files).is_err());
    assert_eq!(files, before);
    for reference in [
        None,
        Some(FileReference::Url {
            url: "https://generativelanguage.googleapis.com/v1beta/files/different"
                .parse()
                .unwrap(),
            media_type: Some("text/plain".into()),
        }),
        Some(FileReference::Provider {
            id: source.id.clone(),
            media_type: None,
        }),
    ] {
        let mut changed = source.clone();
        changed.input_reference = reference;
        assert!(publication.bind_source(&changed, 0, &mut files).is_err());
        assert_eq!(files, before, "exact generation reference is required");
    }
}

#[test]
fn source_binding_requires_prepared_state_and_distinct_batch_input() {
    let source = DeletedFixture::uploaded_file(&DeletedFixture::upload_request());
    for stage in 0..3 {
        let mut publication = DeletedFixture::source_publication(&source);
        publication.begin().unwrap();
        match stage {
            0 => {}
            1 => publication.accepted("job-1").unwrap(),
            2 => publication.rejected().unwrap(),
            _ => unreachable!(),
        }
        let mut files = BTreeMap::from([(source.id.clone(), None)]);
        let before = files.clone();
        assert!(publication.bind_source(&source, 0, &mut files).is_err());
        assert_eq!(files, before, "binding must precede admitted work");
    }
    let mut publication = DeletedFixture::source_publication(&source);
    publication.uploaded_input(&source.id).unwrap();
    let mut files = BTreeMap::from([(source.id.clone(), None)]);
    let before = files.clone();
    assert!(publication.bind_source(&source, 0, &mut files).is_err());
    assert_eq!(files, before, "source cannot double as its JSONL input");
}
