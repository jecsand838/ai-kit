//! Deterministic checks of test predicates, never evidence that a provider works.
use providers_kit::error::{ErrorKind, ErrorPhase, NativeErrorDetails, Operation, RemoteFailure};
mod assertion_cases;
mod hosted_assertions;
mod live_assertions;
mod live_failures;

use providers_kit::batch::{BatchJob, BatchResult, BatchStatus};
use providers_kit::generation::{
    FinishReason, FunctionCall, GenerateEvent, GenerateResponse, Message, Usage,
};
use providers_kit::models::ModelId;

use live_assertions::{
    BatchEvidence, ErrorEvidence, GenerationEvidence, NegativeStreamEvidence, StreamEvidence,
};

struct Fixture;

impl Fixture {
    fn failure(code: &str, message: Option<&str>) -> RemoteFailure {
        RemoteFailure::new(
            ErrorKind::ProviderRejected,
            NativeErrorDetails {
                code: Some(code.into()),
                message: message.map(str::to_owned),
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn completed_stream(response: GenerateResponse) -> StreamEvidence {
        let mut evidence = StreamEvidence::new();
        evidence.observe(Self::start()).unwrap();
        evidence
            .observe(GenerateEvent::ContentDelta {
                delta: GenerationEvidence::text(&response).unwrap(),
            })
            .unwrap();
        evidence
            .observe(GenerateEvent::Completed(Box::new(response)))
            .unwrap();
        evidence
    }

    fn missing_resource_error() -> providers_kit::KitError {
        providers_kit::KitError::new(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(NativeErrorDetails {
            code: Some("model_not_found".into()),
            details: vec![providers_kit::error::NativeDetail::Resource {
                resource_type: None,
                name: "strict-missing".into(),
                owner: None,
                description: None,
            }],
            ..Default::default()
        })
    }

    fn progress() -> Vec<GenerateEvent> {
        vec![
            GenerateEvent::ContentDelta {
                delta: "partial output".into(),
            },
            GenerateEvent::ThinkingDelta {
                delta: "partial reasoning".into(),
            },
            GenerateEvent::Usage(Usage::default()),
            GenerateEvent::HostedTool(Box::new(
                providers_kit::hosted::HostedToolOutput::UrlContext {
                    requested_url: None,
                    retrieved_url: None,
                    status: None,
                    citations: Vec::new(),
                },
            )),
        ]
    }

    fn response(answer: &str) -> GenerateResponse {
        GenerateResponse {
            model: ModelId::new("exact-model").unwrap(),
            message: Message::assistant_text(answer).unwrap(),
            finish_reason: Some(FinishReason::Stop),
            citations: Vec::new(),
            artifacts: Vec::new(),
            hosted_tool_outputs: Vec::new(),
            usage: Usage::default(),
        }
    }

    fn start() -> GenerateEvent {
        GenerateEvent::Started {
            model: ModelId::new("exact-model").unwrap(),
        }
    }

    fn job(status: BatchStatus) -> BatchJob {
        BatchJob {
            id: "job-1".to_owned(),
            status,
            created_at: None,
            completed_at: None,
            usage: None,
        }
    }

    fn succeeded(id: &str, answer: &str) -> BatchResult {
        BatchResult::Succeeded {
            custom_id: id.to_owned(),
            output: Box::new(Self::response(answer)),
        }
    }

    fn expected() -> Vec<(String, String)> {
        vec![
            ("row-a".to_owned(), "answer-a".to_owned()),
            ("row-b".to_owned(), "answer-b".to_owned()),
        ]
    }
}

#[test]
fn exact_answer_and_stop_are_required() {
    assert!(GenerationEvidence::exact_text(&Fixture::response("nonce"), "nonce", false).is_ok());
    for wrong in ["wrong", " nonce", "nonce\n", "prefix nonce suffix"] {
        assert!(GenerationEvidence::exact_text(&Fixture::response(wrong), "nonce", false).is_err());
    }
    for reason in [
        None,
        Some(FinishReason::Length),
        Some(FinishReason::Error),
        Some(FinishReason::ContentFilter),
        Some(FinishReason::Paused),
        Some(FinishReason::ToolCall),
    ] {
        let mut response = Fixture::response("nonce");
        response.finish_reason = reason;
        assert!(GenerationEvidence::exact_text(&response, "nonce", false).is_err());
    }
}

#[test]
fn current_canonical_response_identity_rejects_a_different_model() {
    for name in [
        "gpt-6-astra",
        "gpt-6.1-sol",
        "gpt-6-sol",
        "gpt-6-luna",
        "claude-fable-5-1",
        "claude-opus-5-5",
        "claude-sonnet-5-5",
        "claude-haiku-5-5",
    ] {
        let selected = ModelId::new(name).unwrap();
        let mut response = Fixture::response("answer");
        response.model = selected.clone();
        GenerationEvidence::selected_model(&selected, &response).unwrap();
        response.model = ModelId::new("different-model").unwrap();
        assert!(
            GenerationEvidence::selected_model(&selected, &response).is_err(),
            "canonical selected identity {name} accepted a different response model"
        );
    }
}

#[test]
fn current_canonical_response_identity_does_not_globalize_to_legacy_aliases() {
    for (selected, reported) in [
        ("gpt-4.1", "gpt-4.1-2025-04-14"),
        ("claude-sonnet-4-5", "claude-sonnet-4-5-20250929"),
        ("models/gemini-3.8-flash", "models/gemini-3.8-flash"),
    ] {
        let mut response = Fixture::response("answer");
        response.model = ModelId::new(reported).unwrap();
        GenerationEvidence::selected_model(&ModelId::new(selected).unwrap(), &response).unwrap();
    }
}

#[test]
fn current_canonical_response_identity_checks_tool_only_streams_after_terminal_validation() {
    let call = FunctionCall::new(
        Some("strict-call".into()),
        "strict_lookup",
        serde_json::json!({"value":"providers-kit"}),
    )
    .unwrap();
    let mut response = Fixture::response("unused");
    response.message = Message::assistant(Vec::new(), vec![call]).unwrap();
    response.finish_reason = Some(FinishReason::ToolCall);
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    evidence
        .observe(GenerateEvent::Completed(Box::new(response)))
        .unwrap();
    let response = evidence.finish_function_call().unwrap();
    assert!(
        GenerationEvidence::selected_model(&ModelId::new("gpt-6-luna").unwrap(), &response)
            .is_err(),
        "internally consistent stream identity must still match the selected canonical model"
    );
}

#[test]
fn plain_generation_rejects_unrequested_hosted_activity() {
    let mut response = Fixture::response("nonce");
    response
        .hosted_tool_outputs
        .push(providers_kit::hosted::HostedToolOutput::UrlContext {
            requested_url: None,
            retrieved_url: None,
            status: None,
            citations: Vec::new(),
        });
    assert!(GenerationEvidence::exact_text(&response, "nonce", false).is_err());
}

#[test]
fn stream_requires_exact_order_text_terminal_and_eof() {
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    evidence
        .observe(GenerateEvent::ContentDelta {
            delta: "ans".to_owned(),
        })
        .unwrap();
    evidence
        .observe(GenerateEvent::ContentDelta {
            delta: "wer".to_owned(),
        })
        .unwrap();
    evidence
        .observe(GenerateEvent::Completed(Box::new(Fixture::response(
            "answer",
        ))))
        .unwrap();
    let response = evidence.finish().unwrap();
    GenerationEvidence::exact_text(&response, "answer", false).unwrap();
}

#[test]
fn stream_rejects_incomplete_thinking_only_or_out_of_order_evidence() {
    assert!(StreamEvidence::new().finish().is_err());
    let mut evidence = StreamEvidence::new();
    assert!(
        evidence
            .observe(GenerateEvent::ContentDelta {
                delta: "answer".to_owned()
            })
            .is_err()
    );
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    evidence
        .observe(GenerateEvent::ThinkingDelta {
            delta: "answer".to_owned(),
        })
        .unwrap();
    assert!(evidence.finish().is_err());
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    evidence
        .observe(GenerateEvent::ContentDelta {
            delta: "answer".to_owned(),
        })
        .unwrap();
    assert!(evidence.finish().is_err());
}

#[test]
fn stream_rejects_duplicate_start_wrong_model_and_mismatched_terminal_text() {
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    assert!(evidence.observe(Fixture::start()).is_err());
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    evidence
        .observe(GenerateEvent::ContentDelta {
            delta: "answer".to_owned(),
        })
        .unwrap();
    assert!(
        evidence
            .observe(GenerateEvent::Completed(Box::new(Fixture::response(
                "different"
            ))))
            .is_err()
    );
}

#[test]
fn stream_rejects_duplicate_terminal_or_post_terminal_events() {
    for event in [
        Fixture::start(),
        GenerateEvent::Completed(Box::new(Fixture::response("answer"))),
        GenerateEvent::Usage(Usage::default()),
    ] {
        let mut evidence = StreamEvidence::new();
        evidence.observe(Fixture::start()).unwrap();
        evidence
            .observe(GenerateEvent::ContentDelta {
                delta: "answer".to_owned(),
            })
            .unwrap();
        evidence
            .observe(GenerateEvent::Completed(Box::new(Fixture::response(
                "answer",
            ))))
            .unwrap();
        assert!(evidence.observe(event).is_err());
    }
}

#[test]
fn stream_rejects_unsuccessful_authoritative_completion() {
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    evidence
        .observe(GenerateEvent::ContentDelta {
            delta: "answer".to_owned(),
        })
        .unwrap();
    let mut response = Fixture::response("answer");
    response.finish_reason = Some(FinishReason::Error);
    evidence
        .observe(GenerateEvent::Completed(Box::new(response)))
        .unwrap();
    assert!(evidence.finish().is_err());
}

#[test]
fn structural_stream_completion_keeps_a_failed_response_inspectable() {
    for reason in [FinishReason::Length, FinishReason::Error] {
        let mut response = Fixture::response("usable but unsuccessful answer");
        response.finish_reason = Some(reason.clone());
        response
            .hosted_tool_outputs
            .push(providers_kit::hosted::HostedToolOutput::UrlContext {
                requested_url: None,
                retrieved_url: None,
                status: Some(providers_kit::hosted::HostedToolStatus::Failed {
                    error: Fixture::failure("retrieval_failed", None),
                }),
                citations: Vec::new(),
            });
        assert!(
            Fixture::completed_stream(response.clone())
                .finish()
                .is_err()
        );
        let observed = Fixture::completed_stream(response)
            .finish_structure()
            .unwrap();
        assert_eq!(observed.finish_reason, Some(reason));
        assert_eq!(observed.hosted_tool_outputs.len(), 1);
        assert!(GenerationEvidence::completed(&observed).is_err());
    }
}

#[test]
fn stream_rejects_a_terminal_model_different_from_its_start() {
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    evidence
        .observe(GenerateEvent::ContentDelta {
            delta: "answer".to_owned(),
        })
        .unwrap();
    let mut response = Fixture::response("answer");
    response.model = ModelId::new("different-model").unwrap();
    assert!(
        evidence
            .observe(GenerateEvent::Completed(Box::new(response)))
            .is_err()
    );
}

#[test]
fn batch_requires_successful_status_and_exact_job_identity() {
    BatchEvidence::completed(&Fixture::job(BatchStatus::Completed), "job-1").unwrap();
    assert!(
        BatchEvidence::completed(&Fixture::job(BatchStatus::Completed), "different-job").is_err()
    );
    for status in [
        BatchStatus::Queued,
        BatchStatus::Running,
        BatchStatus::Cancelled,
        BatchStatus::Expired,
        BatchStatus::Failed {
            errors: vec![Fixture::failure("bad_input", Some("failed"))],
        },
    ] {
        assert!(BatchEvidence::completed(&Fixture::job(status), "job-1").is_err());
    }
}

#[test]
fn batch_requires_all_rows_exactly_once_and_correct_answers() {
    BatchEvidence::rows(
        &[
            Fixture::succeeded("row-b", "answer-b"),
            Fixture::succeeded("row-a", "answer-a"),
        ],
        &Fixture::expected(),
    )
    .unwrap();
    for rows in [
        vec![],
        vec![Fixture::succeeded("row-a", "answer-a")],
        vec![
            Fixture::succeeded("row-a", "answer-a"),
            Fixture::succeeded("row-a", "answer-a"),
        ],
        vec![
            Fixture::succeeded("row-a", "answer-a"),
            Fixture::succeeded("row-c", "answer-b"),
        ],
        vec![
            Fixture::succeeded("row-a", "wrong"),
            Fixture::succeeded("row-b", "answer-b"),
        ],
    ] {
        assert!(BatchEvidence::rows(&rows, &Fixture::expected()).is_err());
    }
}

#[test]
fn completed_batch_cannot_hide_unsuccessful_rows() {
    for row in [
        BatchResult::Failed {
            custom_id: "row-b".to_owned(),
            error: Fixture::failure("bad_input", Some("failed")),
        },
        BatchResult::Cancelled {
            custom_id: "row-b".to_owned(),
        },
        BatchResult::Expired {
            custom_id: "row-b".to_owned(),
        },
    ] {
        assert!(
            BatchEvidence::rows(
                &[Fixture::succeeded("row-a", "answer-a"), row],
                &Fixture::expected()
            )
            .is_err()
        );
    }
    let mut response = Fixture::response("answer-b");
    response.finish_reason = Some(FinishReason::Error);
    assert!(
        BatchEvidence::rows(
            &[
                Fixture::succeeded("row-a", "answer-a"),
                BatchResult::Succeeded {
                    custom_id: "row-b".to_owned(),
                    output: Box::new(response),
                }
            ],
            &Fixture::expected()
        )
        .is_err()
    );
}

#[test]
fn stream_preserves_native_snapshot_identity_without_rewriting_it() {
    let mut evidence = StreamEvidence::new();
    evidence
        .observe(GenerateEvent::Started {
            model: ModelId::new("native-snapshot-version").unwrap(),
        })
        .unwrap();
    evidence
        .observe(GenerateEvent::ContentDelta {
            delta: "answer".to_owned(),
        })
        .unwrap();
    let mut response = Fixture::response("answer");
    response.model = ModelId::new("native-snapshot-version").unwrap();
    evidence
        .observe(GenerateEvent::Completed(Box::new(response)))
        .unwrap();
    assert_eq!(
        evidence.finish().unwrap().model.as_str(),
        "native-snapshot-version"
    );
}

#[test]
fn stream_cannot_hide_observed_hosted_activity_or_failure_in_terminal_response() {
    use providers_kit::hosted::{HostedToolOutput, HostedToolStatus};
    for output in [
        HostedToolOutput::UrlContext {
            requested_url: None,
            retrieved_url: None,
            status: Some(HostedToolStatus::InProgress),
            citations: Vec::new(),
        },
        HostedToolOutput::UrlContext {
            requested_url: None,
            retrieved_url: None,
            status: Some(HostedToolStatus::Failed {
                error: Fixture::failure("fetch_failed", Some("failed")),
            }),
            citations: Vec::new(),
        },
    ] {
        let mut evidence = StreamEvidence::new();
        evidence.observe(Fixture::start()).unwrap();
        evidence
            .observe(GenerateEvent::HostedTool(Box::new(output)))
            .unwrap();
        evidence
            .observe(GenerateEvent::ContentDelta {
                delta: "answer".to_owned(),
            })
            .unwrap();
        assert!(
            evidence
                .observe(GenerateEvent::Completed(Box::new(Fixture::response(
                    "answer"
                ))))
                .is_err()
        );
    }
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    evidence
        .observe(GenerateEvent::HostedTool(Box::new(
            HostedToolOutput::UrlContext {
                requested_url: None,
                retrieved_url: None,
                status: Some(HostedToolStatus::Failed {
                    error: Fixture::failure("fetch_failed", Some("failed")),
                }),
                citations: Vec::new(),
            },
        )))
        .unwrap();
    evidence
        .observe(GenerateEvent::ContentDelta {
            delta: "answer".to_owned(),
        })
        .unwrap();
    let mut response = Fixture::response("answer");
    response
        .hosted_tool_outputs
        .push(HostedToolOutput::UrlContext {
            requested_url: None,
            retrieved_url: None,
            status: Some(HostedToolStatus::Completed),
            citations: Vec::new(),
        });
    assert!(
        evidence
            .observe(GenerateEvent::Completed(Box::new(response)))
            .is_err()
    );
}

#[test]
fn missing_model_requires_the_expected_resource_and_provider_error_code() {
    use providers_kit::KitError;
    let make = |status, code: &str, message: &str| {
        KitError::new(
            match status {
                404 => ErrorKind::NotFound,
                401 => ErrorKind::Authentication,
                429 => ErrorKind::RateLimited,
                _ => ErrorKind::ProviderRejected,
            },
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
        )
        .with_http_status(status)
        .with_native_details(NativeErrorDetails {
            code: Some(code.to_owned()),
            message: Some(message.to_owned()),
            ..Default::default()
        })
    };
    let codes = &[Some("model_not_found")];
    assert!(
        ErrorEvidence::not_found(
            &make(
                404,
                "model_not_found",
                "The model `strict-missing` does not exist"
            ),
            Operation::Generate,
            "strict-missing",
            codes
        )
        .is_ok()
    );
    for error in [
        make(
            404,
            "model_not_found",
            "The model `different-model` does not exist",
        ),
        make(404, "route_not_found", "strict-missing"),
        make(401, "model_not_found", "strict-missing"),
        make(429, "rate_limit", "strict-missing"),
        make(500, "model_not_found", "strict-missing"),
    ] {
        assert!(
            ErrorEvidence::not_found(&error, Operation::Generate, "strict-missing", codes).is_err()
        );
    }
}

#[test]
fn missing_resource_accepts_an_exact_structured_identity_without_message_echo() {
    let error = providers_kit::KitError::new(
        ErrorKind::NotFound,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
    )
    .with_http_status(404)
    .with_native_details(NativeErrorDetails {
        code: Some("model_not_found".into()),
        details: vec![providers_kit::error::NativeDetail::Resource {
            resource_type: None,
            name: "strict-missing".into(),
            owner: None,
            description: None,
        }],
        message: Some("The requested resource was not found.".into()),
        ..Default::default()
    });
    assert!(
        ErrorEvidence::not_found(
            &error,
            Operation::Generate,
            "strict-missing",
            &[Some("model_not_found")]
        )
        .is_ok()
    );
}

#[test]
fn missing_resource_message_accepts_sentence_punctuation_without_partial_identifiers() {
    for (message, accepted) in [
        ("Unable to find strict-missing.", true),
        ("Unable to find strict-missing. Please check the ID.", true),
        ("Unable to find strict-missing.extra.", false),
        ("Unable to find prefix-strict-missing.", false),
        ("Unable to find strict-missing-extra.", false),
        ("Unable to find strict-missing/child.", false),
        ("Unable to find strict-missing..", false),
    ] {
        let error = providers_kit::KitError::new(
            ErrorKind::NotFound,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
        )
        .with_http_status(404)
        .with_native_details(NativeErrorDetails {
            code: Some("model_not_found".into()),
            message: Some(message.into()),
            ..Default::default()
        });
        assert_eq!(
            ErrorEvidence::not_found(
                &error,
                Operation::Generate,
                "strict-missing",
                &[Some("model_not_found")],
            )
            .is_ok(),
            accepted,
        );
    }
}

#[test]
fn missing_resource_rejects_a_structured_identity_that_contradicts_message_echo() {
    let error = providers_kit::KitError::new(
        ErrorKind::NotFound,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
    )
    .with_http_status(404)
    .with_native_details(NativeErrorDetails {
        code: Some("model_not_found".into()),
        details: vec![providers_kit::error::NativeDetail::Resource {
            resource_type: None,
            name: "another-resource".into(),
            owner: None,
            description: None,
        }],
        message: Some("The model 'strict-missing' was not found.".into()),
        ..Default::default()
    });
    assert!(
        ErrorEvidence::not_found(
            &error,
            Operation::Generate,
            "strict-missing",
            &[Some("model_not_found")]
        )
        .is_err()
    );
}

#[test]
fn missing_resource_checks_native_codes_and_statuses_independently() {
    for details in [
        NativeErrorDetails {
            native_status: Some("PERMISSION_DENIED".into()),
            ..Default::default()
        },
        NativeErrorDetails {
            reported_http_status: Some(403),
            ..Default::default()
        },
        NativeErrorDetails {
            error_type: Some("authentication_error".into()),
            ..Default::default()
        },
    ] {
        let error = providers_kit::KitError::new(
            ErrorKind::NotFound,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
        )
        .with_http_status(404)
        .with_native_details(NativeErrorDetails {
            code: Some("model_not_found".into()),
            message: Some("The model 'strict-missing' was not found.".into()),
            ..details
        });
        assert!(
            ErrorEvidence::not_found(
                &error,
                Operation::Generate,
                "strict-missing",
                &[Some("model_not_found")]
            )
            .is_err(),
            "a recognized code must not conceal a contradictory native category"
        );
    }
}

#[test]
fn missing_resource_accepts_consistent_native_fields_and_nonproving_envelopes() {
    for envelope in ["error", "invalid_request_error"] {
        let error = providers_kit::KitError::new(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(NativeErrorDetails {
            code: Some("model_not_found".into()),
            error_type: Some(envelope.into()),
            native_status: Some("NOT_FOUND".into()),
            reported_http_status: Some(404),
            details: vec![providers_kit::error::NativeDetail::Resource {
                resource_type: None,
                name: "strict-missing".into(),
                owner: None,
                description: None,
            }],
            ..Default::default()
        });
        assert!(
            ErrorEvidence::not_found(
                &error,
                Operation::GenerateStream,
                "strict-missing",
                &[Some("model_not_found"), Some("NOT_FOUND")],
            )
            .is_ok()
        );
    }
    let error = providers_kit::KitError::new(
        ErrorKind::NotFound,
        Operation::GenerateStream,
        ErrorPhase::Stream,
    )
    .with_native_details(NativeErrorDetails {
        error_type: Some("error".into()),
        details: vec![providers_kit::error::NativeDetail::Resource {
            resource_type: None,
            name: "strict-missing".into(),
            owner: None,
            description: None,
        }],
        ..Default::default()
    });
    assert!(
        ErrorEvidence::not_found(
            &error,
            Operation::GenerateStream,
            "strict-missing",
            &[None, Some("model_not_found")],
        )
        .is_err()
    );
}

#[test]
fn missing_resource_requires_the_exact_expected_generation_operation() {
    for (actual, expected) in [
        (Operation::Generate, Operation::GenerateStream),
        (Operation::GenerateStream, Operation::Generate),
        (Operation::BatchResults, Operation::BatchResults),
    ] {
        let error =
            providers_kit::KitError::new(ErrorKind::NotFound, actual, ErrorPhase::ResponseHeaders)
                .with_http_status(404)
                .with_native_details(NativeErrorDetails {
                    code: Some("model_not_found".into()),
                    details: vec![providers_kit::error::NativeDetail::Resource {
                        resource_type: None,
                        name: "strict-missing".into(),
                        owner: None,
                        description: None,
                    }],
                    ..Default::default()
                });
        assert!(
            ErrorEvidence::not_found(
                &error,
                expected,
                "strict-missing",
                &[Some("model_not_found")],
            )
            .is_err()
        );
    }
}

#[test]
fn missing_resource_failure_diagnostics_do_not_expose_native_strings() {
    let secret = "SENSITIVE_NATIVE_PAYLOAD";
    let error = providers_kit::KitError::new(
        ErrorKind::NotFound,
        Operation::GenerateStream,
        ErrorPhase::Stream,
    )
    .with_native_details(NativeErrorDetails {
        message: Some(secret.into()),
        body: Some(secret.into()),
        code: Some(secret.into()),
        error_type: Some(secret.into()),
        native_status: Some(secret.into()),
        details: vec![providers_kit::error::NativeDetail::Resource {
            resource_type: None,
            name: secret.into(),
            owner: None,
            description: None,
        }],
        request_id: Some(secret.into()),
        ..Default::default()
    });
    let diagnostic = ErrorEvidence::not_found(
        &error,
        Operation::GenerateStream,
        "strict-missing",
        &[Some("model_not_found")],
    )
    .unwrap_err();
    assert!(!diagnostic.contains(secret));
    for check in [
        "classification_matches=false",
        "code_matches=false",
        "identity_matches=false",
    ] {
        assert!(diagnostic.contains(check));
    }
}

#[test]
fn negative_stream_accepts_error_before_start_or_after_partial_progress() {
    let mut early = NegativeStreamEvidence::new();
    early
        .observe(Err(Fixture::missing_resource_error()))
        .unwrap();
    let error = early.finish().unwrap();
    assert_eq!(error.kind(), ErrorKind::NotFound);

    let mut evidence = NegativeStreamEvidence::new();
    evidence.observe(Ok(Fixture::start())).unwrap();
    for event in Fixture::progress() {
        evidence.observe(Ok(event)).unwrap();
    }
    evidence
        .observe(Err(Fixture::missing_resource_error()))
        .unwrap();
    let error = evidence.finish().unwrap();
    ErrorEvidence::not_found(
        &error,
        Operation::GenerateStream,
        "strict-missing",
        &[Some("model_not_found")],
    )
    .unwrap();
}

#[test]
fn negative_stream_rejects_progress_before_start_and_duplicate_start() {
    for event in Fixture::progress() {
        assert!(NegativeStreamEvidence::new().observe(Ok(event)).is_err());
    }
    let mut evidence = NegativeStreamEvidence::new();
    evidence.observe(Ok(Fixture::start())).unwrap();
    assert!(evidence.observe(Ok(Fixture::start())).is_err());
}

#[test]
fn negative_stream_rejects_completion_and_eof_without_error() {
    assert!(NegativeStreamEvidence::new().finish().is_err());
    let mut evidence = NegativeStreamEvidence::new();
    evidence.observe(Ok(Fixture::start())).unwrap();
    assert!(evidence.finish().is_err());

    for reason in [FinishReason::Stop, FinishReason::Error] {
        let mut evidence = NegativeStreamEvidence::new();
        evidence.observe(Ok(Fixture::start())).unwrap();
        let mut response = Fixture::response("answer");
        response.finish_reason = Some(reason);
        assert!(
            evidence
                .observe(Ok(GenerateEvent::Completed(Box::new(response))))
                .is_err()
        );
    }
}

#[test]
fn negative_stream_rejects_every_item_after_its_terminal_error() {
    let mut extra = Fixture::progress().into_iter().map(Ok).collect::<Vec<_>>();
    extra.extend([
        Ok(Fixture::start()),
        Ok(GenerateEvent::Completed(Box::new(Fixture::response(
            "answer",
        )))),
        Err(Fixture::missing_resource_error()),
    ]);
    for item in extra {
        let mut evidence = NegativeStreamEvidence::new();
        evidence
            .observe(Err(Fixture::missing_resource_error()))
            .unwrap();
        assert!(evidence.observe(item).is_err());
    }
}

#[test]
fn negative_stream_preserves_wrong_error_classification_for_scenario_rejection() {
    let mut evidence = NegativeStreamEvidence::new();
    evidence.observe(Ok(Fixture::start())).unwrap();
    evidence
        .observe(Err(providers_kit::KitError::new(
            ErrorKind::QuotaExceeded,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )))
        .unwrap();
    let error = evidence.finish().unwrap();
    assert_eq!(error.kind(), ErrorKind::QuotaExceeded);
    assert!(
        ErrorEvidence::not_found(
            &error,
            Operation::GenerateStream,
            "strict-missing",
            &[Some("model_not_found")],
        )
        .is_err()
    );
}

#[test]
fn missing_model_accepts_http_rejection_and_classified_in_stream_error() {
    for details in [
        NativeErrorDetails {
            code: Some("model_not_found".into()),
            ..Default::default()
        },
        NativeErrorDetails {
            error_type: Some("not_found_error".into()),
            ..Default::default()
        },
        NativeErrorDetails {
            native_status: Some("NOT_FOUND".into()),
            ..Default::default()
        },
        NativeErrorDetails {
            reported_http_status: Some(404),
            ..Default::default()
        },
    ] {
        let details = NativeErrorDetails {
            message: Some("The model 'strict-missing' does not exist".into()),
            ..details
        };
        let codes = &[
            Some("model_not_found"),
            Some("not_found_error"),
            Some("NOT_FOUND"),
            Some("404"),
        ];
        for operation in [Operation::Generate, Operation::GenerateStream] {
            let error = providers_kit::KitError::new(
                ErrorKind::NotFound,
                operation,
                ErrorPhase::ResponseHeaders,
            )
            .with_http_status(404)
            .with_native_details(details.clone());
            assert!(ErrorEvidence::not_found(&error, operation, "strict-missing", codes).is_ok());
        }
        let error = providers_kit::KitError::new(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(details);
        assert!(
            ErrorEvidence::not_found(&error, Operation::GenerateStream, "strict-missing", codes)
                .is_ok(),
            "a recognized scenario code and stream context satisfy the assertion contract without an HTTP error status"
        );
    }
    let error = providers_kit::KitError::new(
        ErrorKind::NotFound,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
    )
    .with_http_status(404)
    .with_native_details(NativeErrorDetails {
        message: Some("The model 'strict-missing' does not exist".into()),
        ..Default::default()
    });
    assert!(
        ErrorEvidence::not_found(&error, Operation::Generate, "strict-missing", &[None]).is_ok()
    );
}

#[test]
fn missing_model_rejects_inconsistent_failure_context() {
    for (kind, operation, phase, status) in [
        (
            ErrorKind::Authentication,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
            Some(404),
        ),
        (
            ErrorKind::NotFound,
            Operation::ModelInfo,
            ErrorPhase::ResponseHeaders,
            Some(404),
        ),
        (
            ErrorKind::NotFound,
            Operation::Unspecified,
            ErrorPhase::ResponseHeaders,
            Some(404),
        ),
        (
            ErrorKind::NotFound,
            Operation::Generate,
            ErrorPhase::RequestBuild,
            Some(404),
        ),
        (
            ErrorKind::NotFound,
            Operation::Generate,
            ErrorPhase::Unspecified,
            Some(404),
        ),
        (
            ErrorKind::NotFound,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
            Some(200),
        ),
        (
            ErrorKind::NotFound,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
            Some(401),
        ),
        (
            ErrorKind::NotFound,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
            None,
        ),
        (
            ErrorKind::NotFound,
            Operation::Generate,
            ErrorPhase::Stream,
            None,
        ),
        (
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Normalize,
            None,
        ),
        (
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
            Some(404),
        ),
        (
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
            Some(200),
        ),
    ] {
        let mut error = providers_kit::KitError::new(kind, operation, phase).with_native_details(
            NativeErrorDetails {
                code: Some("model_not_found".into()),
                message: Some("The model 'strict-missing' does not exist".into()),
                ..Default::default()
            },
        );
        if let Some(status) = status {
            error = error.with_http_status(status);
        }
        let expected_operation = match operation {
            Operation::GenerateStream => Operation::GenerateStream,
            _ => Operation::Generate,
        };
        assert!(
            ErrorEvidence::not_found(
                &error,
                expected_operation,
                "strict-missing",
                &[Some("model_not_found")]
            )
            .is_err(),
            "inconsistent failure context must not prove the scenario: {error:?}"
        );
    }
}

#[test]
fn missing_model_stream_requires_explicit_code_and_exact_resource() {
    for (code, message) in [
        (None, "strict-missing"),
        (Some(""), "strict-missing"),
        (Some(" "), "strict-missing"),
        (Some("insufficient_quota"), "strict-missing"),
        (Some("model_not_found_extra"), "strict-missing"),
        (Some("model_not_found"), "different-model"),
        (Some("model_not_found"), "strict-missing-extra"),
        (Some("model_not_found"), "prefix-strict-missing"),
        (Some("model_not_found"), ""),
    ] {
        let error = providers_kit::KitError::new(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(NativeErrorDetails {
            code: code.map(str::to_owned),
            message: Some(message.into()),
            ..Default::default()
        });
        assert!(
            ErrorEvidence::not_found(
                &error,
                Operation::GenerateStream,
                "strict-missing",
                &[None, Some("model_not_found")]
            )
            .is_err(),
            "an HTTP-only absent-code allowance must not qualify stream errors"
        );
    }
}

#[test]
fn terminal_cannot_replace_an_observed_hosted_family_with_another() {
    use providers_kit::hosted::HostedToolOutput;
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    evidence
        .observe(GenerateEvent::HostedTool(Box::new(
            HostedToolOutput::UrlContext {
                requested_url: None,
                retrieved_url: None,
                status: None,
                citations: Vec::new(),
            },
        )))
        .unwrap();
    evidence
        .observe(GenerateEvent::ContentDelta {
            delta: "answer".to_owned(),
        })
        .unwrap();
    let mut response = Fixture::response("answer");
    response
        .hosted_tool_outputs
        .push(HostedToolOutput::WebSearch {
            status: None,
            action: None,
            results: Vec::new(),
            citations: Vec::new(),
        });
    assert!(
        evidence
            .observe(GenerateEvent::Completed(Box::new(response)))
            .is_err()
    );
}

#[test]
fn missing_resource_accepts_rpc_namespace_and_complete_punctuation_tokens() {
    for message in [
        "files/owned: unavailable",
        "file_id=files/owned",
        "File 'files/owned'.",
    ] {
        let error = providers_kit::KitError::new(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(NativeErrorDetails {
            rpc_code: Some(5),
            message: Some(message.into()),
            ..Default::default()
        });
        assert!(
            ErrorEvidence::not_found(
                &error,
                Operation::GenerateStream,
                "files/owned",
                &[Some("NOT_FOUND")]
            )
            .is_ok()
        );
    }
}

#[test]
fn missing_resource_does_not_use_truncated_or_extended_identity_evidence() {
    let make = |message: &str, truncated| {
        providers_kit::KitError::new(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(NativeErrorDetails {
            code: Some("model_not_found".into()),
            message: Some(message.into()),
            truncated,
            ..Default::default()
        })
    };
    assert!(
        ErrorEvidence::not_found(
            &make("files/owned", true),
            Operation::GenerateStream,
            "files/owned",
            &[Some("model_not_found")]
        )
        .is_err()
    );
    for message in [
        "https://wrong.example/files/owned",
        "https://wrong.example/?file_id=files/owned",
        "files/owned/child",
        "files/owned?extra",
        "files/owned#extra",
        "prefix-files/owned",
        "files/owned-suffix",
    ] {
        assert!(
            ErrorEvidence::not_found(
                &make(message, false),
                Operation::GenerateStream,
                "files/owned",
                &[Some("model_not_found")]
            )
            .is_err(),
            "{message}"
        );
    }
}

#[test]
fn function_call_stream_accepts_tool_only_completion_and_preserves_identity() {
    let call = FunctionCall::new(
        Some("strict-call".into()),
        "strict_lookup",
        serde_json::json!({"value":"providers-kit"}),
    )
    .unwrap();
    let mut response = Fixture::response("unused");
    response.message = Message::assistant(Vec::new(), vec![call.clone()]).unwrap();
    response.finish_reason = Some(FinishReason::ToolCall);
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    evidence
        .observe(GenerateEvent::Completed(Box::new(response)))
        .unwrap();
    let observed = evidence.finish_function_call().unwrap();
    assert_eq!(observed.message.function_calls(), &[call]);
}

#[test]
fn function_call_stream_rejects_wrong_terminal_missing_start_and_model_mismatch() {
    assert!(StreamEvidence::new().finish_function_call().is_err());
    assert!(
        Fixture::completed_stream(Fixture::response("text"))
            .finish_function_call()
            .is_err()
    );
    let mut empty_call = Fixture::response("text");
    empty_call.finish_reason = Some(FinishReason::ToolCall);
    assert!(
        Fixture::completed_stream(empty_call)
            .finish_function_call()
            .is_err()
    );
    let mut evidence = StreamEvidence::new();
    assert!(
        evidence
            .observe(GenerateEvent::Completed(Box::new(Fixture::response(
                "text"
            ))))
            .is_err()
    );
    let mut evidence = StreamEvidence::new();
    evidence.observe(Fixture::start()).unwrap();
    let mut response = Fixture::response("text");
    response.model = ModelId::new("foreign-model").unwrap();
    assert!(
        evidence
            .observe(GenerateEvent::Completed(Box::new(response)))
            .is_err()
    );
}
