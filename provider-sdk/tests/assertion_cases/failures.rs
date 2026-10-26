use provider_sdk::SdkError;
use provider_sdk::error::{ErrorKind, ErrorPhase, NativeErrorDetails, Operation, RemoteFailure};

use crate::live_failures::CaseFailure;

#[test]
fn usable_response_evidence_preserves_public_fields_without_serializing_continuation() {
    use provider_sdk::batch::BatchResult;
    use provider_sdk::generation::{FinishReason, GenerateResponse, Message, Usage};
    use provider_sdk::hosted::{HostedCodeExecutionResult, HostedToolOutput};
    use provider_sdk::models::ModelId;

    let response = GenerateResponse {
        model: ModelId::new("exact-observed-model").unwrap(),
        message: Message::<String>::assistant_text("Answer:\nprivate-observed-answer\n")
            .unwrap()
            .with_continuation("private-opaque-continuation".to_owned())
            .unwrap(),
        finish_reason: Some(FinishReason::Stop),
        citations: Vec::new(),
        artifacts: Vec::new(),
        hosted_tool_outputs: vec![HostedToolOutput::CodeExecution {
            status: None,
            code: Vec::new(),
            results: vec![HostedCodeExecutionResult {
                status: None,
                logs: Some("private-native-log\n".to_owned()),
                stdout: None,
                stderr: None,
                exit_code: None,
            }],
            artifacts: Vec::new(),
        }],
        usage: Usage::default(),
    };
    assert!(serde_json::to_value(&response).is_err());
    let observation = CaseFailure::response_evidence(&response);
    assert_eq!(observation["model"], "exact-observed-model");
    assert_eq!(
        observation["message"]["content"][0]["text"],
        "Answer:\nprivate-observed-answer\n"
    );
    assert_eq!(observation["message"]["continuation_present"], true);
    assert_eq!(
        observation["hosted_tool_outputs"][0]["results"][0]["logs"],
        "private-native-log\n"
    );
    assert!(
        !observation
            .to_string()
            .contains("private-opaque-continuation")
    );
    assert!(response.message.continuation().is_some());

    let rows = vec![
        BatchResult::Succeeded {
            custom_id: "private-success-row".to_owned(),
            output: Box::new(response),
        },
        BatchResult::Cancelled {
            custom_id: "private-cancelled-row".to_owned(),
        },
    ];
    let batch = CaseFailure::batch_evidence("private-job-id", &rows);
    assert_eq!(batch["job_id"], "private-job-id");
    assert_eq!(batch["rows"][0]["custom_id"], "private-success-row");
    assert_eq!(batch["rows"][0]["outcome"], "succeeded");
    assert_eq!(
        batch["rows"][0]["response"]["message"]["content"][0]["text"],
        "Answer:\nprivate-observed-answer\n"
    );
    assert_eq!(batch["rows"][1]["custom_id"], "private-cancelled-row");
    assert_eq!(batch["rows"][1]["outcome"], "cancelled");
    assert!(!batch.to_string().contains("private-opaque-continuation"));
}

#[test]
fn required_observation_export_preserves_complete_private_redacted_proof() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("required.json");
    let observation = serde_json::json!({
        "outcome": "retrieval_persists",
        "retrieval_revocation_proven": false,
        "fixture": { "store_state": "lookup_absent", "source_state": "deleted" },
        "native": { "message": "private-credential", "private-credential": "field" },
    });
    CaseFailure::write_required_observation(&path, observation, &["private-credential".into()])
        .unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        saved,
        serde_json::json!({
            "truncated": false,
            "observation": {
                "outcome": "retrieval_persists",
                "retrieval_revocation_proven": false,
                "fixture": { "store_state": "lookup_absent", "source_state": "deleted" },
                "native": { "message": "<credential-redacted>", "<credential-redacted>": "field" },
            },
        })
    );
    assert!(CaseFailure::write_required_observation(&path, serde_json::json!({}), &[]).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[test]
fn required_observation_export_rejects_oversize_without_creating_a_partial_report() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("required-oversized.json");
    let error = CaseFailure::write_required_observation(
        &path,
        serde_json::json!({
            "outcome": "retrieval_persists",
            "retrieval_revocation_proven": false,
            "native": "x".repeat(1024 * 1024),
        }),
        &[],
    )
    .expect_err("a truncated prefix cannot replace required observation evidence");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        !path.exists(),
        "a failed export must not claim a complete report"
    );
}

#[test]
fn required_observation_export_applies_its_bound_after_credential_redaction() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("required-redacted.json");
    let credential = "private-credential".repeat(1024 * 1024 / 10);
    CaseFailure::write_required_observation(
        &path,
        serde_json::json!({ "native": credential }),
        &[credential],
    )
    .unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved["truncated"], false);
    assert_eq!(saved["observation"]["native"], "<credential-redacted>");
}

#[test]
fn usable_observation_export_is_private_redacted_create_new_and_byte_bounded() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("observation.json");
    let observation = serde_json::json!({
        "answer": "exact  text\nwith native formatting",
        "nested": { "logs": "private-credential", "private-credential": "sensitive key" },
    });
    CaseFailure::write_observation(&path, observation, &["private-credential".into()]).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(saved["truncated"], false);
    assert_eq!(
        saved["observation"]["answer"],
        "exact  text\nwith native formatting"
    );
    assert_eq!(
        saved["observation"]["nested"]["logs"],
        "<credential-redacted>"
    );
    assert_eq!(
        saved["observation"]["nested"]["<credential-redacted>"],
        "sensitive key"
    );
    assert!(
        !String::from_utf8(bytes)
            .unwrap()
            .contains("private-credential")
    );
    assert!(CaseFailure::write_observation(&path, serde_json::json!({}), &[]).is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    let oversized = directory.path().join("oversized.json");
    CaseFailure::write_observation(
        &oversized,
        serde_json::json!({"logs": "private-credential🦀\n".repeat(100_000)}),
        &["private-credential".into()],
    )
    .unwrap();
    let bytes = std::fs::read(oversized).unwrap();
    assert!(bytes.len() <= 1024 * 1024);
    let saved: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(saved["truncated"], true);
    assert_eq!(saved["limit_bytes"], 1024 * 1024);
    assert!(saved["serialized_bytes"].as_u64().unwrap() > 1024 * 1024);
    let prefix = saved["json_prefix"].as_str().unwrap();
    assert!(prefix.contains("<credential-redacted>"));
    assert!(!prefix.contains("private-credential"));
}

#[test]
fn ownership_and_accepted_row_correlation_survive_only_explicit_export() {
    let row = RemoteFailure::new(
        ErrorKind::InvalidRequest,
        NativeErrorDetails {
            code: Some("invalid_file".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let failure = CaseFailure::Group {
        label: "owned cleanup and row failures",
        failures: vec![
            CaseFailure::from("artifact discovery unresolved").with_owner("private-owned-job"),
            CaseFailure::BatchRow {
                custom_id: "private-row-id".into(),
                error: row,
            },
        ],
    };
    let retained = failure.evidence();
    assert_eq!(retained["failures"][0]["owner"], "private-owned-job");
    assert_eq!(
        retained["failures"][0]["failure"]["assertion"],
        "artifact discovery unresolved"
    );
    assert_eq!(
        retained["failures"][1]["batch_row"]["custom_id"],
        "private-row-id"
    );
    assert_eq!(
        retained["failures"][1]["batch_row"]["error"]["details"]["code"],
        "invalid_file"
    );
    let ordinary = format!("{failure:?} {failure}");
    assert!(!ordinary.contains("private-owned-job"));
    assert!(!ordinary.contains("private-row-id"));
}

#[test]
fn case_failures_keep_primary_remote_and_cleanup_evidence_separate_and_redacted() {
    let primary = SdkError::new(
        ErrorKind::NotFound,
        Operation::BatchResults,
        ErrorPhase::ResponseHeaders,
    )
    .with_http_status(404)
    .with_request_target("batch-owned")
    .with_native_details(NativeErrorDetails {
        message: Some("private-primary".into()),
        ..Default::default()
    });
    let cleanup = SdkError::new(
        ErrorKind::Transport,
        Operation::DeleteFile,
        ErrorPhase::RequestSend,
    )
    .with_request_target("file-owned")
    .with_native_details(NativeErrorDetails {
        message: Some("private-cleanup".into()),
        ..Default::default()
    });
    let failure = CaseFailure::combine(Err(primary.into()), Err(cleanup.into())).unwrap_err();
    let ordinary = format!("{failure:?} {failure}");
    assert!(!ordinary.contains("private-primary"));
    assert!(!ordinary.contains("private-cleanup"));
    let retained = failure.evidence();
    assert_eq!(
        retained["failures"][0]["sdk"]["native"]["message"],
        "private-primary"
    );
    assert_eq!(
        retained["failures"][1]["sdk"]["native"]["message"],
        "private-cleanup"
    );
    assert_eq!(
        retained["failures"][1]["sdk"]["request_target"],
        "file-owned"
    );
}

#[test]
fn failed_case_retains_accepted_remote_outcome_without_making_it_an_operation_error() {
    let remote = RemoteFailure::new(
        ErrorKind::InvalidRequest,
        NativeErrorDetails {
            code: Some("invalid_file".into()),
            message: Some("private-row".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let failure = CaseFailure::from("row attribution unproven").with_remote(remote);
    let retained = failure.evidence();
    assert_eq!(
        retained["failures"][0]["assertion"],
        "row attribution unproven"
    );
    assert_eq!(
        retained["failures"][1]["remote"]["details"]["message"],
        "private-row"
    );
    assert!(!format!("{failure:?}").contains("private-row"));
}

#[test]
fn case_failure_preserves_causes_and_supplementary_operation_context() {
    let error = SdkError::new(
        ErrorKind::Transport,
        Operation::Generate,
        ErrorPhase::ReadBody,
    )
    .with_cause(SdkError::new(
        ErrorKind::Timeout,
        Operation::Generate,
        ErrorPhase::ReadBody,
    ))
    .with_supplementary(SdkError::new(
        ErrorKind::Transport,
        Operation::DeleteFile,
        ErrorPhase::RequestSend,
    ));
    let retained = CaseFailure::from(error).evidence();
    assert_eq!(retained["sdk"]["cause"]["context"]["operation"], "Generate");
    assert_eq!(
        retained["sdk"]["supplementary"][0]["context"]["operation"],
        "DeleteFile"
    );
}

#[test]
fn explicit_case_export_redacts_credentials_and_never_overwrites_prior_evidence() {
    let path = std::env::temp_dir().join(format!(
        "provider-case-evidence-{}.json",
        uuid::Uuid::new_v4()
    ));
    let error = SdkError::new(
        ErrorKind::Authentication,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
    )
    .with_native_details(NativeErrorDetails {
        body: Some("token=private-credential".into()),
        ..Default::default()
    });
    let failure = CaseFailure::from("fixture prerequisite failure")
        .with_sdk(error)
        .context("owned fixture");
    failure
        .write_evidence(&path, &["private-credential".into()])
        .unwrap();
    let contents = std::fs::read_to_string(&path).unwrap();
    assert!(!contents.contains("private-credential"));
    assert!(contents.contains("credential-redacted"));
    assert!(failure.write_evidence(&path, &[]).is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    std::fs::remove_file(path).unwrap();
    assert!(CaseFailure::collected("no failures", vec![]).is_ok());
}

#[test]
fn negative_stream_protocol_failure_retains_prior_and_additional_sdk_errors() {
    use crate::live_assertions::NegativeStreamEvidence;
    use provider_sdk::generation::GenerateEvent;
    for second_error in [false, true] {
        let primary = SdkError::new(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(NativeErrorDetails {
            message: Some("private-first-stream-error".into()),
            ..Default::default()
        });
        let mut stream = NegativeStreamEvidence::new();
        stream.observe(Err(primary)).unwrap();
        let next = if second_error {
            Err(SdkError::new(
                ErrorKind::Transport,
                Operation::GenerateStream,
                ErrorPhase::ReadBody,
            )
            .with_native_details(NativeErrorDetails {
                message: Some("private-second-stream-error".into()),
                ..Default::default()
            }))
        } else {
            Ok(GenerateEvent::ContentDelta {
                delta: "unexpected progress".into(),
            })
        };
        let failure: CaseFailure = stream.observe(next).unwrap_err();
        let mut pending = vec![&failure];
        let mut retained = Vec::new();
        while let Some(item) = pending.pop() {
            match item {
                CaseFailure::Sdk(error) => retained.push(error.native_details().message.as_deref()),
                CaseFailure::Group { failures, .. } => pending.extend(failures),
                CaseFailure::Owned { failure, .. } => pending.push(failure),
                CaseFailure::Assertion(_)
                | CaseFailure::Remote(_)
                | CaseFailure::BatchRow { .. } => {}
            }
        }
        assert_eq!(retained.len(), if second_error { 2 } else { 1 });
        assert!(retained.contains(&Some("private-first-stream-error")));
        if second_error {
            assert!(retained.contains(&Some("private-second-stream-error")));
        }
        let ordinary = format!("{failure:?} {failure}");
        assert!(!ordinary.contains("private-first-stream-error"));
        assert!(!ordinary.contains("private-second-stream-error"));
    }
}

#[test]
fn batch_inventory_assertions_do_not_log_native_custom_ids() {
    use crate::Fixture;
    use crate::live_assertions::BatchEvidence;
    let private = "private-untrusted-batch-custom-id";
    let expected = vec![
        ("expected-a".into(), "answer".into()),
        ("expected-b".into(), "answer".into()),
    ];
    for (rows, inventory) in [
        (
            vec![
                Fixture::succeeded("expected-a", "answer"),
                Fixture::succeeded(private, "answer"),
            ],
            expected,
        ),
        (
            vec![
                Fixture::succeeded(private, "answer"),
                Fixture::succeeded(private, "answer"),
            ],
            vec![
                (private.into(), "answer".into()),
                ("expected-b".into(), "answer".into()),
            ],
        ),
    ] {
        let diagnostic = BatchEvidence::rows(&rows, &inventory).unwrap_err();
        assert!(
            !diagnostic.contains(private),
            "ordinary assertion text must omit native row identifiers"
        );
    }
}

#[test]
fn interrupted_negative_stream_retains_rejection_without_claiming_clean_eof() {
    use crate::live_assertions::NegativeStreamEvidence;
    let mut stream = NegativeStreamEvidence::new();
    stream
        .observe(Err(SdkError::new(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(NativeErrorDetails {
            message: Some("private-error-before-deadline".into()),
            ..Default::default()
        })))
        .unwrap();
    let failure = stream.interrupted("negative stream EOF deadline expired");
    assert!(failure.to_string().contains("EOF deadline expired"));
    assert!(!format!("{failure:?} {failure}").contains("private-error-before-deadline"));
    let mut pending = vec![&failure];
    let mut retained = 0;
    while let Some(item) = pending.pop() {
        match item {
            CaseFailure::Sdk(error) => {
                assert_eq!(
                    error.native_details().message.as_deref(),
                    Some("private-error-before-deadline")
                );
                retained += 1;
            }
            CaseFailure::Group { failures, .. } => pending.extend(failures),
            CaseFailure::Owned { failure, .. } => pending.push(failure),
            CaseFailure::Assertion(_) | CaseFailure::Remote(_) | CaseFailure::BatchRow { .. } => {}
        }
    }
    assert_eq!(retained, 1);
    assert!(matches!(
        NegativeStreamEvidence::new().interrupted("opening deadline expired"),
        CaseFailure::Assertion(_)
    ));
}
