use crate::Fixture;
use crate::hosted_assertions::{FileSearchContract, HostedProvider};
use crate::live_assertions::{BatchEvidence, StoreFixture, StreamEvidence};
use crate::live_failures::CaseFailure;
use provider_sdk::SdkError;
use provider_sdk::error::{
    ErrorKind, ErrorPhase, NativeDetail, NativeErrorDetails, Operation, RemoteFailure,
    ResponseViolation,
};
use provider_sdk::files::{FileObject, FileState, FileUploadRequest};
use provider_sdk::generation::{
    Artifact, FileReference, FinishReason, GenerateEvent, GenerateResponse, Message, Usage,
};
use provider_sdk::hosted::{
    HostedCodeExecutionResult, HostedExecutableCode, HostedToolOutput, HostedToolStatus,
    HostedWebSearchAction,
};
use provider_sdk::models::ModelId;

struct StoreRejection;

#[test]
fn store_stream_observation_accepts_completion_only_after_matching_text() {
    let mut stream = StreamEvidence::new();
    stream.observe_result(Ok(Fixture::start())).unwrap();
    stream
        .observe_result(Ok(GenerateEvent::ContentDelta {
            delta: "answer".into(),
        }))
        .unwrap();
    stream
        .observe_result(Ok(GenerateEvent::Completed(Box::new(Fixture::response(
            "answer",
        )))))
        .unwrap();
    let response = stream.finish_observation().unwrap();
    assert_eq!(response.finish_reason, Some(FinishReason::Stop));
    assert_eq!(
        response.message.content(),
        Fixture::response("answer").message.content()
    );
    let report = StoreRejection::prepared(FileSearchContract::Exclusive)
        .consistency_probe(
            Ok(response),
            Operation::GenerateStream,
            StoreRejection::SECRET,
        )
        .unwrap();
    assert_eq!(report["outcome"], "completed_without_rejection");
    assert_eq!(report["native_rejection_observed"], false);
}

#[test]
fn store_stream_observation_holds_native_rejection_until_clean_eof() {
    for partial in [false, true] {
        let mut stream = StreamEvidence::new();
        if partial {
            stream.observe_result(Ok(Fixture::start())).unwrap();
            stream
                .observe_result(Ok(GenerateEvent::ContentDelta {
                    delta: "partial answer".into(),
                }))
                .unwrap();
        }
        let original = StoreRejection::error(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
            None,
            Some("NOT_FOUND"),
        );
        stream
            .observe_result(Err(original.clone()))
            .expect("native rejection must be retained while consuming through EOF");
        let failure = stream.finish_observation().unwrap_err();
        let CaseFailure::Sdk(retained) = &failure else {
            panic!("one native error followed by EOF must preserve its SDK classification");
        };
        assert_eq!(retained.kind(), original.kind());
        assert_eq!(retained.context(), original.context());
        assert_eq!(retained.native_details(), original.native_details());
        let report = StoreRejection::prepared(FileSearchContract::Exclusive)
            .consistency_probe(
                Err(failure),
                Operation::GenerateStream,
                StoreRejection::SECRET,
            )
            .unwrap();
        assert_eq!(report["outcome"], "native_rejection_observed");
        assert_eq!(report["retrieval_revocation_proven"], false);
    }
}

#[test]
fn store_stream_observation_rejects_native_error_after_completion() {
    let mut stream = StreamEvidence::new();
    stream.observe_result(Ok(Fixture::start())).unwrap();
    stream
        .observe_result(Ok(GenerateEvent::ContentDelta {
            delta: "answer".into(),
        }))
        .unwrap();
    stream
        .observe_result(Ok(GenerateEvent::Completed(Box::new(Fixture::response(
            "answer",
        )))))
        .unwrap();
    let error = StoreRejection::error(
        ErrorKind::NotFound,
        Operation::GenerateStream,
        ErrorPhase::Stream,
        None,
        Some("NOT_FOUND"),
    );
    let failure = stream.observe_result(Err(error)).unwrap_err();
    assert!(
        !matches!(&failure, CaseFailure::Sdk(_)),
        "an error after Completed is a protocol failure, not an eligible rejection"
    );
    let evidence = failure.evidence().to_string();
    assert!(evidence.contains("NOT_FOUND"));
    assert!(evidence.contains("\"assertion\":"));
    assert!(
        StoreRejection::prepared(FileSearchContract::Exclusive)
            .consistency_probe(
                Err(failure),
                Operation::GenerateStream,
                StoreRejection::SECRET,
            )
            .is_err()
    );
}

#[test]
fn store_stream_observation_rejects_post_error_events_and_retains_failures() {
    let secondary = SdkError::new(
        ErrorKind::Transport,
        Operation::GenerateStream,
        ErrorPhase::Stream,
    )
    .with_native_details(NativeErrorDetails {
        message: Some("secondary-stream-failure".into()),
        ..Default::default()
    });
    for next in [
        Ok(Fixture::start()),
        Ok(GenerateEvent::ContentDelta {
            delta: "unexpected text".into(),
        }),
        Ok(GenerateEvent::ThinkingDelta {
            delta: "unexpected reasoning".into(),
        }),
        Ok(GenerateEvent::Usage(Usage::default())),
        Ok(GenerateEvent::HostedTool(Box::new(
            HostedToolOutput::FileSearch {
                status: None,
                queries: Vec::new(),
                results: Some(Vec::new()),
                citations: Vec::new(),
            },
        ))),
        Ok(GenerateEvent::Completed(Box::new(Fixture::response(
            "answer",
        )))),
        Err(secondary),
    ] {
        let repeated_error = next.is_err();
        let original = StoreRejection::error(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
            None,
            Some("NOT_FOUND"),
        );
        let mut stream = StreamEvidence::new();
        stream.observe_result(Err(original)).unwrap();
        let failure = stream.observe_result(next).unwrap_err();
        assert!(!matches!(&failure, CaseFailure::Sdk(_)));
        let evidence = failure.evidence().to_string();
        assert!(evidence.contains("\"assertion\":"));
        assert!(evidence.contains("The store fileSearchStores/server-id is unavailable."));
        assert!(evidence.contains("NOT_FOUND"));
        if repeated_error {
            assert!(evidence.contains("secondary-stream-failure"));
        }
        assert!(
            StoreRejection::prepared(FileSearchContract::Exclusive)
                .consistency_probe(
                    Err(failure),
                    Operation::GenerateStream,
                    StoreRejection::SECRET,
                )
                .is_err()
        );
    }
}

#[test]
fn store_stream_observation_rejects_premature_eof() {
    for events in [
        Vec::new(),
        vec![Fixture::start()],
        vec![
            Fixture::start(),
            GenerateEvent::ContentDelta {
                delta: "unfinished answer".into(),
            },
        ],
    ] {
        let mut stream = StreamEvidence::new();
        for event in events {
            stream.observe_result(Ok(event)).unwrap();
        }
        let failure = stream.finish_observation().unwrap_err();
        assert!(matches!(&failure, CaseFailure::Assertion(_)));
        assert!(
            StoreRejection::prepared(FileSearchContract::Exclusive)
                .consistency_probe(
                    Err(failure),
                    Operation::GenerateStream,
                    StoreRejection::SECRET,
                )
                .is_err()
        );
    }
}

#[test]
fn store_stream_observation_interruption_retains_unsettled_native_error() {
    let mut stream = StreamEvidence::new();
    let error = StoreRejection::error(
        ErrorKind::NotFound,
        Operation::GenerateStream,
        ErrorPhase::Stream,
        None,
        Some("NOT_FOUND"),
    );
    stream.observe_result(Err(error)).unwrap();
    let failure = stream.interrupted("stream observation EOF deadline expired");
    assert!(!matches!(&failure, CaseFailure::Sdk(_)));
    let evidence = failure.evidence().to_string();
    assert!(evidence.contains("\"assertion\":"));
    assert!(evidence.contains("stream observation EOF deadline expired"));
    assert!(evidence.contains("The store fileSearchStores/server-id is unavailable."));
    assert!(evidence.contains("NOT_FOUND"));
    assert!(
        StoreRejection::prepared(FileSearchContract::Exclusive)
            .consistency_probe(
                Err(failure),
                Operation::GenerateStream,
                StoreRejection::SECRET,
            )
            .is_err(),
        "a retained native error without EOF cannot prove the probe outcome"
    );
}

#[test]
fn store_probe_records_persistent_retrieval_without_claiming_rejection_or_erasure() {
    let mut fixture = StoreRejection::prepared(FileSearchContract::Exclusive);
    let report = fixture
        .consistency_probe(
            Ok(StoreRejection::grounded()),
            Operation::Generate,
            StoreRejection::SECRET,
        )
        .unwrap();
    assert_eq!(report["outcome"], "retrieval_persists");
    assert_eq!(report["native_rejection_observed"], false);
    assert_eq!(report["retrieval_revocation_proven"], false);
    assert_eq!(report["marker_returned"], true);
    assert_eq!(report["retrieval_evidence"], true);
    let absence = fixture.observation();
    assert_eq!(absence["store_state"], "lookup_absent");
    assert_eq!(
        absence["management_absence"]["retrieval_revocation_proven"],
        false
    );
    assert!(
        absence["management_absence"]["checked_at_unix_ms"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[test]
fn store_probe_does_not_turn_prose_or_missing_marker_into_revocation_proof() {
    let mut fixture = StoreRejection::prepared(FileSearchContract::Exclusive);
    let report = fixture
        .consistency_probe(
            Ok(StoreRejection::response(Vec::new())),
            Operation::GenerateStream,
            StoreRejection::SECRET,
        )
        .unwrap();
    assert_eq!(report["outcome"], "completed_without_rejection");
    assert_eq!(report["native_rejection_observed"], false);
    assert_eq!(report["retrieval_revocation_proven"], false);
    assert_eq!(report["marker_returned"], false);
    assert_eq!(report["retrieval_evidence"], false);
}

#[test]
fn store_probe_preserves_correlated_rejection_and_rejects_other_errors() {
    let mut fixture = StoreRejection::prepared(FileSearchContract::Exclusive);
    for operation in [Operation::Generate, Operation::GenerateStream] {
        let error = StoreRejection::error(
            ErrorKind::NotFound,
            operation,
            ErrorPhase::ResponseHeaders,
            Some(404),
            Some("NOT_FOUND"),
        );
        let report = fixture
            .consistency_probe(Err(error.into()), operation, StoreRejection::SECRET)
            .unwrap();
        assert_eq!(report["outcome"], "native_rejection_observed");
        assert_eq!(report["native_rejection_observed"], true);
        assert_eq!(report["retrieval_revocation_proven"], false);
        assert!(report["error"].is_object());
        for kind in [
            ErrorKind::Authentication,
            ErrorKind::PermissionDenied,
            ErrorKind::RateLimited,
            ErrorKind::Transport,
            ErrorKind::Timeout,
            ErrorKind::InvalidResponse(ResponseViolation::MalformedJson),
        ] {
            let error = StoreRejection::error(
                kind,
                operation,
                ErrorPhase::ResponseHeaders,
                Some(404),
                Some("NOT_FOUND"),
            );
            let failure = fixture
                .consistency_probe(Err(error.into()), operation, StoreRejection::SECRET)
                .unwrap_err();
            assert!(
                failure.evidence().to_string().contains("NOT_FOUND"),
                "original native error must survive"
            );
        }
    }
    assert!(
        fixture
            .consistency_probe(
                Err("unexpected stream EOF".into()),
                Operation::GenerateStream,
                StoreRejection::SECRET
            )
            .is_err()
    );
    let mut response = StoreRejection::grounded();
    response.finish_reason = Some(FinishReason::Length);
    assert!(
        fixture
            .consistency_probe(Ok(response), Operation::Generate, StoreRejection::SECRET)
            .is_err()
    );
}

#[test]
fn store_absence_rejects_wrong_resource_auth_and_incomplete_native_evidence() {
    let valid = StoreRejection::absence();
    for error in [
        valid.clone().with_http_status(403),
        valid.clone().with_request_target("fileSearchStores/other"),
        valid.clone().with_native_details(NativeErrorDetails { body: Some(r#"{"error":{"code":404,"status":"NOT_FOUND","message":"fileSearchStores/other not found."}}"#.into()), ..Default::default() }),
        valid.clone().with_native_details(NativeErrorDetails { body: Some(r#"{"error":{"code":403,"status":"PERMISSION_DENIED","message":"fileSearchStores/server-id may not exist."}}"#.into()), ..Default::default() }),
        valid.clone().with_native_details(NativeErrorDetails { truncated: true, ..valid.native_details().clone() }),
        valid.clone().with_native_details(NativeErrorDetails { omitted_details: 1, ..valid.native_details().clone() }),
        valid.clone().with_native_details(NativeErrorDetails::default()),
    ] {
        let mut fixture = StoreRejection::deleted(FileSearchContract::Exclusive);
        assert!(fixture.accept_absence(error).is_err());
        assert!(fixture.consistency_probe(Ok(StoreRejection::grounded()), Operation::Generate, StoreRejection::SECRET).is_err());
    }
    let mut before_delete = StoreRejection::imported(FileSearchContract::Exclusive);
    assert!(before_delete.accept_absence(valid).is_err());
}

#[test]
fn store_probe_requires_lookup_absence_after_deletion_acknowledgement() {
    let fixture = StoreRejection::deleted(FileSearchContract::Exclusive);
    let error = StoreRejection::error(
        ErrorKind::NotFound,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
        Some(404),
        Some("NOT_FOUND"),
    );
    assert!(
        fixture.rejection(&error, Operation::Generate).is_err(),
        "DELETE acknowledgement alone cannot establish the probe prerequisite"
    );
}

impl StoreRejection {
    const SECRET: &str = "undisclosed-file-marker";
    const QUERY_SOURCE: &str = "print(file_search.query(\"strict-test fixture marker\"))\n";

    fn import_source(fixture: &mut StoreFixture) {
        fixture.begin_upload().unwrap();
        fixture
            .accept_uploaded("files/owned-source".into())
            .unwrap();
        assert_eq!(fixture.begin_import().unwrap(), "files/owned-source");
        assert!(
            fixture
                .accept_import(
                    br#"{"name":"fileSearchStores/server-id/operations/import-1","done":true}"#
                )
                .unwrap()
        );
    }

    fn imported(contract: FileSearchContract) -> StoreFixture {
        let mut fixture = Self::created(contract);
        Self::import_source(&mut fixture);
        fixture
    }

    fn grounded() -> GenerateResponse {
        let mut response = Self::response(vec![HostedToolOutput::FileSearch {
            status: None,
            queries: Vec::new(),
            results: Some(Vec::new()),
            citations: Vec::new(),
        }]);
        response.message = Message::assistant_text(format!("Answer: {}", Self::SECRET)).unwrap();
        response
    }

    fn grounded_query_declaration() -> GenerateResponse {
        let mut response = Self::grounded();
        response.model = ModelId::new("models/gemini-2.5-flash").unwrap();
        response
            .hosted_tool_outputs
            .push(HostedToolOutput::CodeExecution {
                status: None,
                code: vec![HostedExecutableCode {
                    language: Some("PYTHON".into()),
                    code: Self::QUERY_SOURCE.into(),
                }],
                results: Vec::new(),
                artifacts: Vec::new(),
            });
        response
    }

    fn created(contract: FileSearchContract) -> StoreFixture {
        let mut fixture = StoreFixture::new("case-nonce".into(), contract);
        fixture.begin_create().unwrap();
        fixture
            .accept_created(br#"{"name":"fileSearchStores/server-id"}"#)
            .unwrap();
        fixture
    }

    fn response(outputs: Vec<HostedToolOutput>) -> GenerateResponse {
        GenerateResponse {
            model: ModelId::new("assertion-fixture").unwrap(),
            message: Message::assistant_text("I could not find the strict-test fixture marker.")
                .unwrap(),
            finish_reason: Some(FinishReason::Stop),
            citations: Vec::new(),
            artifacts: Vec::new(),
            hosted_tool_outputs: outputs,
            usage: Usage::default(),
        }
    }

    fn deleted(contract: FileSearchContract) -> StoreFixture {
        let mut fixture = Self::imported(contract);
        fixture
            .accept_preflight(&Self::grounded(), Self::SECRET)
            .unwrap();
        fixture.begin_delete().unwrap();
        fixture.accept_deleted(b"{}").unwrap();
        fixture
    }

    fn absence() -> SdkError {
        SdkError::new(ErrorKind::ProviderRejected, Operation::Unspecified, ErrorPhase::ResponseHeaders)
            .with_http_status(404)
            .with_request_target("fileSearchStores/server-id")
            .with_native_details(NativeErrorDetails {
                body: Some(r#"{"error":{"code":404,"status":"NOT_FOUND","message":"fileSearchStores/server-id not found."}}"#.into()),
                ..Default::default()
            })
    }

    fn prepared(contract: FileSearchContract) -> StoreFixture {
        let mut fixture = Self::deleted(contract);
        fixture.accept_absence(Self::absence()).unwrap();
        fixture
    }

    fn error(
        kind: ErrorKind,
        operation: Operation,
        phase: ErrorPhase,
        status: Option<u16>,
        native: Option<&str>,
    ) -> SdkError {
        let mut error =
            SdkError::new(kind, operation, phase).with_native_details(NativeErrorDetails {
                native_status: native.map(str::to_owned),
                message: Some("The store fileSearchStores/server-id is unavailable.".into()),
                ..Default::default()
            });
        if let Some(status) = status {
            error = error.with_http_status(status);
        }
        error
    }
}

#[test]
fn store_preflight_rejects_completed_text_without_retrieval_evidence() {
    let mut fixture = StoreRejection::imported(FileSearchContract::Exclusive);
    let response = StoreRejection::response(Vec::new());
    assert!(
        fixture
            .accept_preflight(&response, StoreRejection::SECRET)
            .is_err()
    );
    fixture.begin_delete().unwrap();
    fixture.accept_deleted(b"{}").unwrap();
    assert!(fixture.negative_target().is_err());
}

#[test]
fn store_preflight_rejects_code_declarations_without_file_search_evidence() {
    let mut fixture = StoreRejection::imported(FileSearchContract::Exclusive);
    let response = StoreRejection::response(vec![HostedToolOutput::CodeExecution {
        status: None,
        code: vec![HostedExecutableCode {
            language: Some("PYTHON".into()),
            code: "print(file_search.query(query=\"strict-test fixture marker\"))\n".into(),
        }],
        results: Vec::new(),
        artifacts: Vec::new(),
    }]);
    assert!(
        fixture
            .accept_preflight(&response, StoreRejection::SECRET)
            .is_err()
    );
    fixture.begin_delete().unwrap();
    fixture.accept_deleted(b"{}").unwrap();
    assert!(fixture.negative_target().is_err());
}

#[test]
fn store_preflight_accepts_identity_free_grounding_only_with_the_undisclosed_marker() {
    let mut fixture = StoreRejection::imported(FileSearchContract::Exclusive);
    let response = StoreRejection::grounded();
    fixture
        .accept_preflight(&response, StoreRejection::SECRET)
        .unwrap();
    assert_eq!(fixture.observation()["retrieval_preflight"], true);
    assert!(fixture.negative_target().is_err());
    fixture.begin_delete().unwrap();
    fixture.accept_deleted(b"{}").unwrap();
    assert_eq!(
        fixture.negative_target().unwrap(),
        "fileSearchStores/server-id"
    );
}

#[test]
fn store_preflight_rejects_code_activity_alongside_valid_file_search() {
    let declaration = HostedExecutableCode {
        language: Some("PYTHON".into()),
        code: "print(\"probe\")".into(),
    };
    let result = HostedCodeExecutionResult {
        status: Some(HostedToolStatus::Completed),
        logs: Some("probe\n".into()),
        stdout: None,
        stderr: None,
        exit_code: None,
    };
    for (code, results) in [
        (Vec::new(), Vec::new()),
        (vec![declaration.clone()], Vec::new()),
        (vec![declaration], vec![result.clone()]),
        (Vec::new(), vec![result]),
    ] {
        let mut fixture = StoreRejection::imported(FileSearchContract::Exclusive);
        let mut response = StoreRejection::grounded();
        response
            .hosted_tool_outputs
            .push(HostedToolOutput::CodeExecution {
                status: None,
                code,
                results,
                artifacts: Vec::new(),
            });
        assert!(
            fixture
                .accept_preflight(&response, StoreRejection::SECRET)
                .is_err(),
            "a correct marker and file-search grounding cannot authorize an unexpected code family"
        );
        assert_eq!(fixture.observation()["retrieval_preflight"], false);
        fixture.begin_delete().unwrap();
        fixture.accept_deleted(b"{}").unwrap();
        assert!(fixture.negative_target().is_err());
        assert_eq!(
            fixture.begin_input_delete().as_deref(),
            Some("files/owned-source")
        );
    }
}

#[test]
fn store_preflight_rejects_other_hosted_families_alongside_valid_file_search() {
    for extra in [
        HostedToolOutput::WebSearch {
            status: None,
            action: Some(HostedWebSearchAction::Search {
                query: Some("strict-test fixture marker".into()),
                queries: Vec::new(),
                sources: Vec::new(),
            }),
            results: Vec::new(),
            citations: Vec::new(),
        },
        HostedToolOutput::UrlContext {
            requested_url: Some("https://www.iana.org/help/example-domains".into()),
            retrieved_url: Some("https://www.iana.org/help/example-domains".into()),
            status: Some(HostedToolStatus::Completed),
            citations: Vec::new(),
        },
    ] {
        let mut fixture = StoreRejection::imported(FileSearchContract::Exclusive);
        let mut response = StoreRejection::grounded();
        response.hosted_tool_outputs.push(extra);
        assert!(
            fixture
                .accept_preflight(&response, StoreRejection::SECRET)
                .is_err(),
            "file-search-only preflight must reject every unrelated hosted family"
        );
        assert_eq!(fixture.observation()["retrieval_preflight"], false);
        fixture.begin_delete().unwrap();
        fixture.accept_deleted(b"{}").unwrap();
        assert!(fixture.negative_target().is_err());
        assert_eq!(
            fixture.begin_input_delete().as_deref(),
            Some("files/owned-source")
        );
    }
}

#[test]
fn store_preflight_rejects_wrong_marker_missing_grounding_and_unsuccessful_generation() {
    let mut wrong_marker = StoreRejection::grounded();
    wrong_marker.message = Message::assistant_text("Answer: guessed-marker").unwrap();
    let mut no_evidence = StoreRejection::grounded();
    no_evidence.hosted_tool_outputs.clear();
    let mut length = StoreRejection::grounded();
    length.finish_reason = Some(FinishReason::Length);
    let mut error = StoreRejection::grounded();
    error.finish_reason = Some(FinishReason::Error);
    let mut missing_answer = StoreRejection::grounded();
    missing_answer.message = Message::assistant_text("The source has a marker.").unwrap();
    let mut wrong_family = StoreRejection::grounded();
    wrong_family.hosted_tool_outputs.clear();
    wrong_family
        .hosted_tool_outputs
        .push(HostedToolOutput::CodeExecution {
            status: None,
            code: Vec::new(),
            results: Vec::new(),
            artifacts: Vec::new(),
        });
    for response in [
        wrong_marker,
        no_evidence,
        length,
        error,
        missing_answer,
        wrong_family,
    ] {
        let mut fixture = StoreRejection::imported(FileSearchContract::Exclusive);
        assert!(
            fixture
                .accept_preflight(&response, StoreRejection::SECRET)
                .is_err()
        );
        fixture.begin_delete().unwrap();
        fixture.accept_deleted(b"{}").unwrap();
        assert!(fixture.negative_target().is_err());
    }
}

#[test]
fn store_preflight_preserves_failed_retrieval_and_rejects_invented_status() {
    for status in [
        HostedToolStatus::Incomplete,
        HostedToolStatus::InProgress,
        HostedToolStatus::Completed,
        HostedToolStatus::Failed {
            error: RemoteFailure::new(
                ErrorKind::ProviderRejected,
                NativeErrorDetails {
                    message: Some("native retrieval failed".into()),
                    ..Default::default()
                },
            )
            .unwrap(),
        },
    ] {
        let mut response = StoreRejection::grounded();
        let HostedToolOutput::FileSearch {
            status: observed, ..
        } = &mut response.hosted_tool_outputs[0]
        else {
            panic!("file search fixture");
        };
        *observed = Some(status);
        let mut fixture = StoreRejection::imported(FileSearchContract::Exclusive);
        assert!(
            fixture
                .accept_preflight(&response, StoreRejection::SECRET)
                .is_err()
        );
        assert!(
            !fixture.observation()["retrieval_preflight"]
                .as_bool()
                .unwrap()
        );
    }
}

#[test]
fn store_import_readiness_requires_exact_owned_operation_and_a_valid_result_union() {
    for body in [
        br#"{}"#.as_slice(),
        br#"{"name":"fileSearchStores/foreign/operations/import-1","done":true}"#.as_slice(),
        br#"{"name":"fileSearchStores/server-id/operations/../other","done":true}"#.as_slice(),
        br#"{"name":"fileSearchStores/server-id/operations/import-1","done":"true"}"#.as_slice(),
        br#"{"name":"fileSearchStores/server-id/operations/import-1","done":false,"error":{"code":13}}"#.as_slice(),
        br#"{"name":"fileSearchStores/server-id/operations/import-1","response":[]}"#.as_slice(),
        br#"{"name":"fileSearchStores/server-id/operations/import-1","done":true,"response":[],"error":null}"#.as_slice(),
        br#"{"name":"fileSearchStores/server-id/operations/import-1","done":true,"response":{},"error":{"code":13}}"#.as_slice(),
        br#"{"name":"fileSearchStores/server-id/operations/import-1","done":true,"error":"failed"}"#.as_slice(),
    ] {
        let mut fixture = StoreRejection::created(FileSearchContract::Exclusive);
        fixture.begin_upload().unwrap();
        fixture.accept_uploaded("files/owned-source".into()).unwrap();
        fixture.begin_import().unwrap();
        assert!(fixture.accept_import(body).is_err());
        assert!(fixture.accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET).is_err());
        assert!(fixture.begin_import().is_err());
        assert_eq!(fixture.begin_input_delete().as_deref(), Some("files/owned-source"));
    }
}

#[test]
fn store_import_provisional_response_stays_pending_until_explicit_completion() {
    for response in [
        r#"{"@type":"type.googleapis.com/google.ai.generativelanguage.v1main.ImportFileResponse"}"#,
        r#"{"documentName":"fileSearchStores/server-id/documents/premature","status":"COMPLETED"}"#,
    ] {
        for done in ["", ",\"done\":false", ",\"done\":null"] {
            let mut fixture = StoreRejection::created(FileSearchContract::Exclusive);
            fixture.begin_upload().unwrap();
            fixture
                .accept_uploaded("files/owned-source".into())
                .unwrap();
            fixture.begin_import().unwrap();
            let body = format!(
                "{{\"name\":\"fileSearchStores/server-id/operations/import-1\"{done},\"response\":{response}}}"
            );
            assert!(!fixture.accept_import(body.as_bytes()).unwrap());
            assert_eq!(
                fixture.import_operation().unwrap(),
                "fileSearchStores/server-id/operations/import-1"
            );
            assert!(fixture.begin_import().is_err());
            assert!(
                fixture
                    .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
                    .is_err()
            );
            assert!(
                !fixture.observation()["retrieval_preflight"]
                    .as_bool()
                    .unwrap()
            );
            let completed = br#"{"name":"fileSearchStores/server-id/operations/import-1","done":true,"response":{}}"#;
            assert!(fixture.accept_import(completed).unwrap());
            fixture
                .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
                .unwrap();
        }
    }
}

#[test]
fn store_import_provisional_response_does_not_hide_terminal_failure() {
    let mut fixture = StoreRejection::created(FileSearchContract::Exclusive);
    fixture.begin_upload().unwrap();
    fixture
        .accept_uploaded("files/owned-source".into())
        .unwrap();
    fixture.begin_import().unwrap();
    assert!(
        !fixture
            .accept_import(
                br#"{"name":"fileSearchStores/server-id/operations/import-1","response":{"@type":"type.googleapis.com/google.ai.generativelanguage.v1main.ImportFileResponse"}}"#,
            )
            .unwrap()
    );
    let error = fixture
        .accept_import(
            br#"{"name":"fileSearchStores/server-id/operations/import-1","done":true,"error":{"code":13,"message":"indexing failed after provisional response"}}"#,
        )
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ProviderRejected);
    assert_eq!(error.native_details().rpc_code, Some(13));
    assert_eq!(
        error.native_details().message.as_deref(),
        Some("indexing failed after provisional response")
    );
    assert!(fixture.import_operation().is_err());
    assert!(
        fixture
            .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
            .is_err()
    );
    assert_eq!(
        fixture.begin_input_delete().as_deref(),
        Some("files/owned-source")
    );
}

#[test]
fn store_import_polls_one_identity_and_accepts_documented_terminal_absent_result() {
    for final_result in ["", ",\"response\":{}", ",\"response\":null,\"error\":null"] {
        let mut fixture = StoreRejection::created(FileSearchContract::Exclusive);
        fixture.begin_upload().unwrap();
        fixture
            .accept_uploaded("files/owned-source".into())
            .unwrap();
        fixture.begin_import().unwrap();
        assert!(
            !fixture
                .accept_import(br#"{"name":"fileSearchStores/server-id/operations/import-1"}"#)
                .unwrap()
        );
        assert_eq!(
            fixture.import_operation().unwrap(),
            "fileSearchStores/server-id/operations/import-1"
        );
        let error = fixture
            .accept_import(
                br#"{"name":"fileSearchStores/server-id/operations/replacement","done":true}"#,
            )
            .unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::InvalidResponse(ResponseViolation::IdentityMismatch { field: "name" })
        );
        let body = format!(
            "{{\"name\":\"fileSearchStores/server-id/operations/import-1\",\"done\":true{final_result}}}"
        );
        assert!(fixture.accept_import(body.as_bytes()).unwrap());
        assert!(fixture.import_operation().is_err());
        assert!(fixture.accept_import(body.as_bytes()).is_err());
        fixture
            .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
            .unwrap();
    }
}

#[test]
fn store_import_failure_preserves_native_error_and_does_not_admit_preflight() {
    let mut fixture = StoreRejection::created(FileSearchContract::Exclusive);
    fixture.begin_upload().unwrap();
    fixture
        .accept_uploaded("files/owned-source".into())
        .unwrap();
    fixture.begin_import().unwrap();
    let body = br#"{"name":"fileSearchStores/server-id/operations/import-1","done":true,"error":{"code":13,"message":"native indexing failure"}}"#;
    let error = fixture.accept_import(body).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ProviderRejected);
    assert_eq!(error.native_details().rpc_code, Some(13));
    assert_eq!(
        error.native_details().message.as_deref(),
        Some("native indexing failure")
    );
    assert_eq!(
        error.native_details().body.as_deref(),
        Some(std::str::from_utf8(body).unwrap())
    );
    assert_eq!(
        error.request_target(),
        Some("fileSearchStores/server-id/operations/import-1")
    );
    assert!(
        fixture
            .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
            .is_err()
    );
    assert!(fixture.import_operation().is_err());
    assert_eq!(
        fixture.begin_input_delete().as_deref(),
        Some("files/owned-source")
    );
}

#[test]
fn store_source_cleanup_remains_independent_after_failed_preflight_and_store_delete() {
    let mut fixture = StoreRejection::imported(FileSearchContract::Exclusive);
    assert!(
        fixture
            .accept_preflight(
                &StoreRejection::response(Vec::new()),
                StoreRejection::SECRET
            )
            .is_err()
    );
    assert!(fixture.begin_delete().unwrap().is_some());
    assert!(fixture.finish().is_err());
    assert_eq!(
        fixture.begin_input_delete().as_deref(),
        Some("files/owned-source")
    );
    assert!(fixture.finish_input().is_err());
    assert_eq!(fixture.begin_input_delete(), None);
    fixture.accept_input_deleted().unwrap();
    fixture.finish_input().unwrap();
    assert!(fixture.finish().is_err());
    assert_eq!(fixture.begin_delete().unwrap(), None);
}

#[test]
fn store_deletion_does_not_discharge_source_cleanup_or_repeat_unknown_mutations() {
    let mut fixture = StoreRejection::created(FileSearchContract::Exclusive);
    fixture.begin_upload().unwrap();
    assert!(fixture.begin_upload().is_err());
    assert!(fixture.begin_import().is_err());
    assert!(fixture.finish_input().is_err());
    assert_eq!(fixture.begin_input_delete(), None);
    fixture
        .accept_uploaded("files/owned-source".into())
        .unwrap();
    fixture.begin_import().unwrap();
    assert!(fixture.begin_import().is_err());
    assert!(fixture.import_operation().is_err());
    fixture.begin_delete().unwrap();
    fixture.accept_deleted(b"{}").unwrap();
    fixture.finish().unwrap();
    assert_eq!(
        fixture.finish_input().unwrap_err().request_target(),
        Some("files/owned-source")
    );
    assert_eq!(
        fixture.begin_input_delete().as_deref(),
        Some("files/owned-source")
    );
    assert!(
        fixture
            .accept_import(
                br#"{"name":"fileSearchStores/server-id/operations/import-1","done":true}"#
            )
            .is_err()
    );
    fixture.accept_input_deleted().unwrap();
    fixture.finish_input().unwrap();
    assert_eq!(fixture.begin_input_delete(), None);
}

#[test]
fn deleted_store_accepts_precise_missing_or_invalid_argument_after_complete_history() {
    let fixture = StoreRejection::prepared(FileSearchContract::Exclusive);
    for (kind, status, native) in [
        (ErrorKind::NotFound, 404, "NOT_FOUND"),
        (ErrorKind::InvalidRequest, 400, "INVALID_ARGUMENT"),
    ] {
        for operation in [Operation::Generate, Operation::GenerateStream] {
            let error = StoreRejection::error(
                kind,
                operation,
                ErrorPhase::ResponseHeaders,
                Some(status),
                Some(native),
            );
            fixture.rejection(&error, operation).unwrap();
        }
        let error = StoreRejection::error(
            kind,
            Operation::GenerateStream,
            ErrorPhase::Stream,
            None,
            Some(native),
        );
        fixture
            .rejection(&error, Operation::GenerateStream)
            .unwrap();
    }
}

#[test]
fn deleted_store_rejects_generic_errors_wrong_mode_and_inconsistent_native_facts() {
    let fixture = StoreRejection::prepared(FileSearchContract::Exclusive);
    for (kind, operation, phase, status, native) in [
        (
            ErrorKind::InvalidRequest,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
            Some(400),
            None,
        ),
        (
            ErrorKind::Authentication,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
            Some(401),
            Some("INVALID_ARGUMENT"),
        ),
        (
            ErrorKind::InvalidRequest,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
            Some(403),
            Some("INVALID_ARGUMENT"),
        ),
        (
            ErrorKind::InvalidRequest,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
            Some(404),
            Some("INVALID_ARGUMENT"),
        ),
        (
            ErrorKind::InvalidRequest,
            Operation::Generate,
            ErrorPhase::RequestBuild,
            Some(400),
            Some("INVALID_ARGUMENT"),
        ),
        (
            ErrorKind::InvalidRequest,
            Operation::GenerateStream,
            ErrorPhase::Stream,
            Some(400),
            Some("INVALID_ARGUMENT"),
        ),
        (
            ErrorKind::InvalidRequest,
            Operation::GenerateStream,
            ErrorPhase::Stream,
            None,
            Some("RESOURCE_EXHAUSTED"),
        ),
        (
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Normalize,
            None,
            Some("NOT_FOUND"),
        ),
    ] {
        let error = StoreRejection::error(kind, operation, phase, status, native);
        assert!(fixture.rejection(&error, operation).is_err());
    }
    let error = StoreRejection::error(
        ErrorKind::InvalidRequest,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
        Some(400),
        Some("INVALID_ARGUMENT"),
    );
    assert!(
        fixture
            .rejection(&error, Operation::GenerateStream)
            .is_err()
    );
    for details in [
        NativeErrorDetails {
            native_status: Some("INVALID_ARGUMENT".into()),
            rpc_code: Some(7),
            message: Some("fileSearchStores/server-id".into()),
            ..Default::default()
        },
        NativeErrorDetails {
            native_status: Some("INVALID_ARGUMENT".into()),
            code: Some("quota_exceeded".into()),
            message: Some("fileSearchStores/server-id".into()),
            ..Default::default()
        },
        NativeErrorDetails {
            native_status: Some("INVALID_ARGUMENT".into()),
            message: Some("fileSearchStores/server-id-extra".into()),
            ..Default::default()
        },
        NativeErrorDetails {
            native_status: Some("INVALID_ARGUMENT".into()),
            message: Some("fileSearchStores/server-id".into()),
            truncated: true,
            ..Default::default()
        },
        NativeErrorDetails {
            native_status: Some("INVALID_ARGUMENT".into()),
            message: Some("fileSearchStores/server-id".into()),
            details: vec![NativeDetail::Resource {
                resource_type: None,
                name: "fileSearchStores/other".into(),
                owner: None,
                description: None,
            }],
            ..Default::default()
        },
    ] {
        let error = error.clone().with_native_details(details);
        assert!(fixture.rejection(&error, Operation::Generate).is_err());
    }
}

#[test]
fn deleted_store_local_request_identity_cannot_replace_native_attribution_or_preflight() {
    let error = StoreRejection::error(
        ErrorKind::InvalidRequest,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
        Some(400),
        Some("INVALID_ARGUMENT"),
    );
    let mut fixture = StoreFixture::new("case-nonce".into(), FileSearchContract::Exclusive);
    fixture.begin_create().unwrap();
    fixture
        .accept_created(br#"{"name":"fileSearchStores/server-id"}"#)
        .unwrap();
    fixture.begin_delete().unwrap();
    fixture.accept_deleted(b"{}").unwrap();
    assert!(fixture.rejection(&error, Operation::Generate).is_err());
    let error = error
        .with_request_target("fileSearchStores/server-id")
        .with_native_details(NativeErrorDetails {
            native_status: Some("INVALID_ARGUMENT".into()),
            ..Default::default()
        });
    assert!(
        StoreRejection::prepared(FileSearchContract::Exclusive)
            .rejection(&error, Operation::Generate)
            .is_err()
    );
}

#[test]
fn store_fixture_never_retries_an_uncertain_create_or_guesses_an_owned_name() {
    let mut fixture = StoreFixture::new("case-nonce".into(), FileSearchContract::Exclusive);
    assert_eq!(fixture.display_name(), "case-nonce");
    fixture.begin_create().unwrap();
    assert!(fixture.begin_create().is_err());
    assert!(fixture.target().is_err());
    assert_eq!(fixture.begin_delete().unwrap(), None);
    let error = fixture.finish().unwrap_err();
    assert_eq!(error.request_target(), None);
    assert_eq!(error.correlation_id(), Some("case-nonce"));
}

#[test]
fn store_fixture_tracks_the_exact_created_name_until_deletion_is_acknowledged() {
    let mut fixture = StoreFixture::new("case-nonce".into(), FileSearchContract::Exclusive);
    fixture.begin_create().unwrap();
    fixture
        .accept_created(
            br#"{"name":"fileSearchStores/server-assigned-01","displayName":"case-nonce"}"#,
        )
        .unwrap();
    assert_eq!(
        fixture.target().unwrap(),
        "fileSearchStores/server-assigned-01"
    );
    assert_eq!(
        fixture.begin_delete().unwrap(),
        Some("fileSearchStores/server-assigned-01".into())
    );
    let error = fixture.finish().unwrap_err();
    assert_eq!(
        error.request_target(),
        Some("fileSearchStores/server-assigned-01")
    );
    fixture.accept_deleted(b"{}").unwrap();
    fixture.finish().unwrap();
    assert_eq!(fixture.begin_delete().unwrap(), None);
    assert_eq!(
        fixture.target().unwrap(),
        "fileSearchStores/server-assigned-01"
    );
}

#[test]
fn store_fixture_never_retries_a_delete_after_transport_or_acknowledgement_failure() {
    for body in [
        None,
        Some(b"{\"unexpected\":true}".as_slice()),
        Some(b"null".as_slice()),
        Some(b"not-json".as_slice()),
    ] {
        let mut fixture = StoreFixture::new("case-nonce".into(), FileSearchContract::Exclusive);
        fixture.begin_create().unwrap();
        fixture
            .accept_created(br#"{"name":"fileSearchStores/server-id"}"#)
            .unwrap();
        assert!(fixture.begin_delete().unwrap().is_some());
        if let Some(body) = body {
            assert!(fixture.accept_deleted(body).is_err());
        }
        assert_eq!(fixture.begin_delete().unwrap(), None);
        let error = fixture.finish().unwrap_err();
        assert_eq!(error.request_target(), Some("fileSearchStores/server-id"));
    }
}

#[test]
fn store_fixture_accepts_only_empty_or_empty_object_delete_acknowledgements() {
    for body in [
        b"".as_slice(),
        b" \r\n\t".as_slice(),
        b"{}".as_slice(),
        b" { } \n".as_slice(),
    ] {
        let mut fixture = StoreFixture::new("case-nonce".into(), FileSearchContract::Exclusive);
        fixture.begin_create().unwrap();
        fixture
            .accept_created(br#"{"name":"fileSearchStores/server-id"}"#)
            .unwrap();
        fixture.begin_delete().unwrap();
        fixture.accept_deleted(body).unwrap();
        fixture.finish().unwrap();
        assert!(fixture.accept_deleted(body).is_err());
    }
}

#[test]
fn store_fixture_rejects_unowned_names_and_malformed_creation_acknowledgements() {
    for body in [
        br#"{}"#.as_slice(),
        br#"{"name":null}"#.as_slice(),
        br#"{"name":"files/server-id"}"#.as_slice(),
        br#"{"name":"fileSearchStores/"}"#.as_slice(),
        br#"{"name":"fileSearchStores/server-id/documents/child"}"#.as_slice(),
        br#"{"name":"fileSearchStores/../files/foreign"}"#.as_slice(),
        br#"{"name":"https://example.com/fileSearchStores/server-id"}"#.as_slice(),
        br#"{"name":"fileSearchStores/server-id?force=true"}"#.as_slice(),
        br#"{"name":"fileSearchStores/server-id%2fchild"}"#.as_slice(),
        b"not-json".as_slice(),
    ] {
        let mut fixture = StoreFixture::new("case-nonce".into(), FileSearchContract::Exclusive);
        fixture.begin_create().unwrap();
        assert!(fixture.accept_created(body).is_err());
        assert_eq!(fixture.begin_delete().unwrap(), None);
        assert_eq!(fixture.finish().unwrap_err().request_target(), None);
    }
}

#[test]
fn store_fixture_rejects_out_of_order_creation_and_delete_acknowledgements() {
    let mut fixture = StoreFixture::new("case-nonce".into(), FileSearchContract::Exclusive);
    assert!(
        fixture
            .accept_created(br#"{"name":"fileSearchStores/server-id"}"#)
            .is_err()
    );
    assert!(fixture.accept_deleted(b"{}").is_err());
    assert_eq!(fixture.begin_delete().unwrap(), None);
    fixture.finish().unwrap();
    fixture.begin_create().unwrap();
    fixture
        .accept_created(br#"{"name":"fileSearchStores/server-id"}"#)
        .unwrap();
    assert!(fixture.begin_create().is_err());
    assert!(
        fixture
            .accept_created(br#"{"name":"fileSearchStores/replacement"}"#)
            .is_err()
    );
    assert!(fixture.accept_deleted(b"{}").is_err());
    assert_eq!(fixture.target().unwrap(), "fileSearchStores/server-id");
    assert!(fixture.finish().is_err());
}

#[test]
fn store_fixture_negative_requires_successful_sdk_preflight_before_acknowledged_deletion() {
    for preflight in [false, true] {
        let mut fixture = StoreFixture::new("case-nonce".into(), FileSearchContract::Exclusive);
        assert!(
            fixture
                .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
                .is_err()
        );
        fixture.begin_create().unwrap();
        assert!(
            fixture
                .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
                .is_err()
        );
        fixture
            .accept_created(br#"{"name":"fileSearchStores/server-id"}"#)
            .unwrap();
        assert!(fixture.negative_target().is_err());
        StoreRejection::import_source(&mut fixture);
        if preflight {
            fixture
                .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
                .unwrap();
            assert!(
                fixture
                    .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
                    .is_err()
            );
        }
        fixture.begin_delete().unwrap();
        assert!(fixture.negative_target().is_err());
        assert!(
            fixture
                .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
                .is_err()
        );
        fixture.accept_deleted(b"{}").unwrap();
        assert!(
            fixture
                .accept_preflight(&StoreRejection::grounded(), StoreRejection::SECRET)
                .is_err()
        );
        if preflight {
            assert_eq!(
                fixture.negative_target().unwrap(),
                "fileSearchStores/server-id"
            );
        } else {
            assert!(fixture.negative_target().is_err());
        }
        fixture.finish().unwrap();
    }
}

#[test]
fn store_metadata_failure_retains_observations_without_redirecting_source_cleanup() {
    let request = FileUploadRequest::new(
        "owned-source.txt",
        "text/plain",
        "secret-only-in-source-café".as_bytes().to_vec(),
    )
    .unwrap();
    let uploaded = FileObject {
        id: "files/owned-source".into(),
        file_name: Some(request.file_name().into()),
        size_bytes: Some(request.data().len() as u64),
        media_type: Some(request.media_type().into()),
        created_at: None,
        state: Some(FileState::Processing),
        input_reference: Some(FileReference::Url {
            url: "https://generativelanguage.googleapis.com/v1beta/files/owned-source"
                .parse()
                .unwrap(),
            media_type: Some(request.media_type().into()),
        }),
    };
    let mut fixture = StoreRejection::created(FileSearchContract::Exclusive);
    fixture.begin_upload().unwrap();
    fixture.observe_source_file(&request, &uploaded);
    fixture.accept_uploaded(uploaded.id.clone()).unwrap();
    let mut lookup = uploaded.clone();
    lookup.id = "files/unrelated-source".into();
    lookup.state = Some(FileState::Active);
    fixture.observe_source_file(&request, &lookup);
    assert!(
        BatchEvidence::fixture_file_ready(&lookup, &uploaded, &request, HostedProvider::Gemini,)
            .is_err()
    );
    assert_eq!(
        fixture.begin_input_delete().as_deref(),
        Some(uploaded.id.as_str())
    );
    fixture.accept_input_deleted().unwrap();
    fixture.finish_input().unwrap();
    let observation = fixture.observation();
    assert_eq!(observation["source_state"], "deleted");
    assert_eq!(observation["source"], uploaded.id);
    let rows = observation["source_file_observations"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["stage"], "upload_acknowledged");
    assert_eq!(rows[1]["stage"], "readiness_lookup");
    assert_eq!(rows[0]["file"], serde_json::to_value(&uploaded).unwrap());
    assert_eq!(rows[1]["file"], serde_json::to_value(&lookup).unwrap());
    assert_eq!(rows[1]["request"]["size_bytes"], request.data().len());
    assert!(
        !observation
            .to_string()
            .contains("secret-only-in-source-café"),
        "metadata evidence must not copy the source payload"
    );
}

#[test]
fn store_gemini_streaming_qualification_accepts_preflight_without_altering_response() {
    let mut fixture = StoreRejection::imported(FileSearchContract::GeminiStreaming);
    let response = StoreRejection::grounded_query_declaration();
    let original = CaseFailure::response_evidence(&response);
    fixture
        .accept_preflight(&response, StoreRejection::SECRET)
        .expect("honest retrieval with the approved unpaired query declaration qualifies");
    assert_eq!(CaseFailure::response_evidence(&response), original);
    let observed = fixture.observation();
    assert_eq!(observed["retrieval_preflight"], true);
    let entries = observed["file_search_observations"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["stage"], "preflight");
    assert_eq!(
        entries[0]["contract"],
        "gemini_streaming_unpaired_file_search_declaration"
    );
    assert_eq!(entries[0]["strict_file_search_only_requirement_met"], false);
    assert_eq!(
        entries[0]["unpaired_file_search_declaration_observed"],
        true
    );
    assert_eq!(entries[0]["code_execution_claimed"], false);
    assert_eq!(entries[0]["response"], original);
    assert!(fixture.negative_target().is_err());
    fixture.begin_delete().unwrap();
    fixture.accept_deleted(b"{}").unwrap();
    fixture.accept_absence(StoreRejection::absence()).unwrap();
    assert_eq!(
        fixture.observation()["management_absence"]["retrieval_revocation_proven"],
        false
    );
    assert_eq!(
        fixture.begin_input_delete().as_deref(),
        Some("files/owned-source")
    );
    fixture.accept_input_deleted().unwrap();
    fixture.finish_input().unwrap();
    fixture.finish().unwrap();
}

#[test]
fn store_gemini_streaming_qualification_records_persistent_retrieval_and_original_code() {
    let mut fixture = StoreRejection::prepared(FileSearchContract::GeminiStreaming);
    assert_eq!(
        fixture.begin_input_delete().as_deref(),
        Some("files/owned-source")
    );
    fixture.accept_input_deleted().unwrap();
    let response = StoreRejection::grounded_query_declaration();
    let original = CaseFailure::response_evidence(&response);
    let report = fixture
        .consistency_probe(
            Ok(response),
            Operation::GenerateStream,
            StoreRejection::SECRET,
        )
        .expect("qualified retrieval remains an observation after acknowledged deletion");
    assert_eq!(report["outcome"], "retrieval_persists");
    assert_eq!(report["marker_returned"], true);
    assert_eq!(report["marker_observed"], true);
    assert_eq!(
        report.get("answer_contract_error"),
        Some(&serde_json::Value::Null)
    );
    assert_eq!(report["retrieval_evidence"], true);
    assert_eq!(report["native_rejection_observed"], false);
    assert_eq!(report["retrieval_revocation_proven"], false);
    assert_eq!(
        report["contract"],
        "gemini_streaming_unpaired_file_search_declaration"
    );
    assert_eq!(report["strict_file_search_only_requirement_met"], false);
    assert_eq!(report["unpaired_file_search_declaration_observed"], true);
    assert_eq!(report["code_execution_claimed"], false);
    assert_eq!(report["response"], original);
    assert_eq!(
        report["response"]["hosted_tool_outputs"][1]["code"][0]["code"],
        StoreRejection::QUERY_SOURCE
    );
    let observed = fixture.observation();
    let entries = observed["file_search_observations"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["stage"], "preflight");
    assert_eq!(entries[0]["strict_file_search_only_requirement_met"], true);
    assert_eq!(
        entries[0]["unpaired_file_search_declaration_observed"],
        false
    );
    assert_eq!(entries[1]["stage"], "post_delete");
    assert_eq!(entries[1]["strict_file_search_only_requirement_met"], false);
    assert_eq!(
        entries[1]["unpaired_file_search_declaration_observed"],
        true
    );
    assert_eq!(entries[1]["code_execution_claimed"], false);
    assert_eq!(entries[1]["response"], original);
    fixture.finish_input().unwrap();
    fixture.finish().unwrap();
}

#[test]
fn store_gemini_streaming_qualification_preserves_marker_prose_observation() {
    for text in [
        format!(
            "The fixture document records the marker {}.",
            StoreRejection::SECRET
        ),
        format!("Retrieved marker: ({}).", StoreRejection::SECRET),
    ] {
        let mut fixture = StoreRejection::prepared(FileSearchContract::GeminiStreaming);
        assert_eq!(
            fixture.begin_input_delete().as_deref(),
            Some("files/owned-source")
        );
        fixture.accept_input_deleted().unwrap();
        let mut response = StoreRejection::grounded_query_declaration();
        response.message = Message::assistant_text(text).unwrap();
        let original = CaseFailure::response_evidence(&response);
        let report = fixture
            .consistency_probe(
                Ok(response),
                Operation::GenerateStream,
                StoreRejection::SECRET,
            )
            .expect("post-deletion prose must retain its format failure as observation evidence");
        assert_eq!(report["outcome"], "completed_without_rejection");
        assert_eq!(report["marker_returned"], false);
        assert_eq!(report["marker_observed"], true);
        assert_eq!(
            report["answer_contract_error"],
            "answer-format: missing Answer: line"
        );
        assert_eq!(report["retrieval_evidence"], true);
        assert_eq!(report["native_rejection_observed"], false);
        assert_eq!(report["retrieval_revocation_proven"], false);
        assert_eq!(report["strict_file_search_only_requirement_met"], false);
        assert_eq!(report["unpaired_file_search_declaration_observed"], true);
        assert_eq!(report["code_execution_claimed"], false);
        assert_eq!(report["response"], original);
        assert_eq!(
            report["response"]["hosted_tool_outputs"][1]["code"][0]["code"],
            StoreRejection::QUERY_SOURCE
        );
        let observed = fixture.observation();
        assert_eq!(observed["retrieval_preflight"], true);
        assert_eq!(observed["store_state"], "lookup_absent");
        assert_eq!(observed["source_state"], "deleted");
        assert_eq!(
            observed["file_search_observations"][1]["response"],
            original
        );
        fixture.finish_input().unwrap();
        fixture.finish().unwrap();
    }
}

#[test]
fn store_gemini_streaming_qualification_rejects_execution_artifacts_and_other_activity() {
    let artifact = Artifact::Inline {
        media_type: "text/plain".into(),
        file_name: Some("unexpected.txt".into()),
        description: None,
        data: b"unexpected artifact".to_vec(),
    };
    for mutation in [
        "execution_result",
        "code_artifact",
        "response_artifact",
        "unrelated_code",
        "missing_file_search",
        "web_search",
        "url_context",
    ] {
        let mut response = StoreRejection::grounded_query_declaration();
        match mutation {
            "execution_result" | "code_artifact" | "unrelated_code" => {
                let HostedToolOutput::CodeExecution {
                    code,
                    results,
                    artifacts,
                    ..
                } = &mut response.hosted_tool_outputs[1]
                else {
                    panic!("fixture query declaration must retain its code family");
                };
                match mutation {
                    "execution_result" => results.push(HostedCodeExecutionResult {
                        status: Some(HostedToolStatus::Completed),
                        logs: Some("actual execution output".into()),
                        stdout: None,
                        stderr: None,
                        exit_code: None,
                    }),
                    "code_artifact" => artifacts.push(artifact.clone()),
                    "unrelated_code" => code[0].code = "print(338350)\n".into(),
                    _ => unreachable!(),
                }
            }
            "response_artifact" => response.artifacts.push(artifact.clone()),
            "missing_file_search" => {
                response.hosted_tool_outputs.remove(0);
            }
            "web_search" => response
                .hosted_tool_outputs
                .push(HostedToolOutput::WebSearch {
                    status: None,
                    action: Some(HostedWebSearchAction::Search {
                        query: Some("strict-test fixture marker".into()),
                        queries: Vec::new(),
                        sources: Vec::new(),
                    }),
                    results: Vec::new(),
                    citations: Vec::new(),
                }),
            "url_context" => response
                .hosted_tool_outputs
                .push(HostedToolOutput::UrlContext {
                    requested_url: Some("https://www.iana.org/help/example-domains".into()),
                    retrieved_url: Some("https://www.iana.org/help/example-domains".into()),
                    status: Some(HostedToolStatus::Completed),
                    citations: Vec::new(),
                }),
            _ => unreachable!(),
        }
        let original = CaseFailure::response_evidence(&response);
        let mut fixture = StoreRejection::imported(FileSearchContract::GeminiStreaming);
        assert!(
            fixture
                .accept_preflight(&response, StoreRejection::SECRET)
                .is_err(),
            "qualification must not admit {mutation}"
        );
        assert_eq!(CaseFailure::response_evidence(&response), original);
        let observed = fixture.observation();
        assert_eq!(observed["retrieval_preflight"], false);
        let entries = observed["file_search_observations"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["response"], original);
        fixture.begin_delete().unwrap();
        fixture.accept_deleted(b"{}").unwrap();
        assert!(fixture.negative_target().is_err());
        assert!(fixture.accept_absence(StoreRejection::absence()).is_err());
        assert!(
            StoreRejection::prepared(FileSearchContract::GeminiStreaming)
                .consistency_probe(
                    Ok(response),
                    Operation::GenerateStream,
                    StoreRejection::SECRET,
                )
                .is_err(),
            "post-deletion observation must not admit {mutation}"
        );
        assert_eq!(
            fixture.begin_input_delete().as_deref(),
            Some("files/owned-source")
        );
        fixture.accept_input_deleted().unwrap();
        fixture.finish_input().unwrap();
        fixture.finish().unwrap();
    }
}

#[test]
fn store_gemini_streaming_qualification_requires_import_and_correct_secret() {
    let response = StoreRejection::grounded_query_declaration();
    let mut before_import = StoreRejection::created(FileSearchContract::GeminiStreaming);
    assert!(
        before_import
            .accept_preflight(&response, StoreRejection::SECRET)
            .is_err()
    );
    assert_eq!(before_import.observation()["retrieval_preflight"], false);
    let mut wrong_marker = response.clone();
    wrong_marker.message = Message::assistant_text("Answer: guessed-marker").unwrap();
    let mut incomplete = response;
    incomplete.finish_reason = Some(FinishReason::Length);
    for response in [wrong_marker, incomplete] {
        let mut fixture = StoreRejection::imported(FileSearchContract::GeminiStreaming);
        assert!(
            fixture
                .accept_preflight(&response, StoreRejection::SECRET)
                .is_err()
        );
        assert_eq!(fixture.observation()["retrieval_preflight"], false);
        fixture.begin_delete().unwrap();
        fixture.accept_deleted(b"{}").unwrap();
        assert!(fixture.negative_target().is_err());
        assert!(fixture.accept_absence(StoreRejection::absence()).is_err());
        assert!(
            StoreRejection::prepared(FileSearchContract::GeminiStreaming)
                .consistency_probe(
                    Ok(response),
                    Operation::GenerateStream,
                    StoreRejection::SECRET,
                )
                .is_err(),
            "a query declaration requires the exact marker and successful finish after deletion"
        );
    }
    for text in [
        "The marker is unavailable.".to_owned(),
        "The marker is guessed-marker.".to_owned(),
        format!("The marker is x{}.", StoreRejection::SECRET),
        format!("The marker is {}x.", StoreRejection::SECRET),
        format!("The marker is prefix_{}.", StoreRejection::SECRET),
        format!("The marker is {}_suffix.", StoreRejection::SECRET),
        format!("The marker is prefix-{}.", StoreRejection::SECRET),
        format!("The marker is {}-suffix.", StoreRejection::SECRET),
        format!("The marker is é{}.", StoreRejection::SECRET),
        format!("The marker is {}界.", StoreRejection::SECRET),
        format!("{} and {}.", StoreRejection::SECRET, StoreRejection::SECRET),
        format!(
            "Answer: {}\nRepeated marker: {}.",
            StoreRejection::SECRET,
            StoreRejection::SECRET,
        ),
    ] {
        let mut response = StoreRejection::grounded_query_declaration();
        response.message = Message::assistant_text(text).unwrap();
        assert!(
            StoreRejection::prepared(FileSearchContract::GeminiStreaming)
                .consistency_probe(
                    Ok(response),
                    Operation::GenerateStream,
                    StoreRejection::SECRET,
                )
                .is_err(),
            "qualified prose requires the exact standalone marker once, without attached text or duplication"
        );
    }
}

#[test]
fn store_exclusive_contract_rejects_even_an_unpaired_file_search_query() {
    let response = StoreRejection::grounded_query_declaration();
    let mut fixture = StoreRejection::imported(FileSearchContract::Exclusive);
    assert!(
        fixture
            .accept_preflight(&response, StoreRejection::SECRET)
            .is_err()
    );
    assert_eq!(fixture.observation()["retrieval_preflight"], false);
    assert!(
        StoreRejection::prepared(FileSearchContract::Exclusive)
            .consistency_probe(
                Ok(response),
                Operation::GenerateStream,
                StoreRejection::SECRET,
            )
            .is_err()
    );
}
