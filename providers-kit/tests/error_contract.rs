use providers_kit::KitError;
use providers_kit::batch::{BatchResult, BatchStatus};
use providers_kit::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, NativeDetail, NativeErrorDetails, Operation,
    RemoteFailure, ResponseDetails, ResponseViolation, ValidationViolation,
};
use providers_kit::hosted::{HostedToolOutput, HostedToolStatus};

#[test]
fn diagnostic_resolution_local_validation_identifies_the_failed_field() {
    let temperature = providers_kit::generation::GenerationConfig::default()
        .with_temperature(-1.0)
        .expect_err("negative temperature is invalid");
    let top_p = providers_kit::generation::GenerationConfig::default()
        .with_top_p(2.0)
        .expect_err("probability above one is invalid");
    assert_eq!(
        temperature.validation_violation(),
        Some(&ValidationViolation::OutOfRange {
            field: "generation.temperature",
            rule: "finite and non-negative"
        })
    );
    assert_eq!(temperature.kind(), ErrorKind::InvalidInput);
    assert_eq!(top_p.kind(), ErrorKind::InvalidInput);
    assert!(temperature.to_string().contains("temperature"));
    assert!(top_p.to_string().contains("top_p"));
    assert_ne!(temperature.to_string(), top_p.to_string());
}

#[test]
fn diagnostic_resolution_error_trees_bound_cause_depth_and_supplementary_nodes() {
    let mut chain = KitError::new(
        ErrorKind::Transport,
        Operation::Generate,
        ErrorPhase::ReadBody,
    );
    for _ in 0..40 {
        chain = KitError::new(
            ErrorKind::Timeout,
            Operation::Generate,
            ErrorPhase::RequestSend,
        )
        .with_cause(chain);
    }
    let mut depth = 1;
    let mut current = &chain;
    while let Some(cause) = current.cause() {
        depth += 1;
        current = cause;
    }
    assert_eq!(
        depth, 8,
        "root plus cause descendants share one depth bound"
    );
    assert!(chain.diagnostics_truncated());

    let mut primary = KitError::new(
        ErrorKind::Authentication,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
    )
    .with_http_status(401);
    for _ in 0..40 {
        primary = primary.with_supplementary(KitError::new(
            ErrorKind::Transport,
            Operation::Generate,
            ErrorPhase::Cleanup,
        ));
    }
    assert_eq!(primary.kind(), ErrorKind::Authentication);
    assert_eq!(primary.http_status(), Some(401));
    assert_eq!(
        primary.supplementary().len(),
        31,
        "root consumes one of 32 nodes"
    );
}

#[test]
fn diagnostic_resolution_error_tree_strings_share_one_capture_budget() {
    let mut primary = KitError::new(
        ErrorKind::Authentication,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
    );
    for _ in 0..25 {
        primary = primary.with_supplementary(
            KitError::new(
                ErrorKind::Transport,
                Operation::Generate,
                ErrorPhase::Cleanup,
            )
            .with_native_details(NativeErrorDetails {
                message: Some("é".repeat(32 * 1024)),
                ..Default::default()
            }),
        );
    }
    let captured = primary
        .supplementary()
        .iter()
        .map(|child| {
            child
                .native_details()
                .message
                .as_ref()
                .map_or(0, String::len)
        })
        .sum::<usize>();
    assert!(
        captured <= 1024 * 1024,
        "the whole tree retained {captured} bytes"
    );
    assert!(primary.diagnostics_truncated());
    assert!(primary.clone().diagnostics_truncated());
}

#[test]
fn diagnostic_resolution_typed_validation_keeps_values_private() {
    for violation in [
        ValidationViolation::MissingField {
            field: "request.input",
        },
        ValidationViolation::Empty {
            field: "request.input",
        },
        ValidationViolation::InvalidFormat {
            field: "request.input",
            rule: "valid JSON",
        },
        ValidationViolation::InvalidValue {
            field: "request.input",
            rule: "known value",
        },
        ValidationViolation::OutOfRange {
            field: "request.input",
            rule: "positive",
        },
        ValidationViolation::Duplicate {
            field: "request.input",
        },
        ValidationViolation::LimitExceeded {
            field: "request.input",
            limit: 8,
        },
        ValidationViolation::Incompatible {
            field: "request.input",
            other: "request.output",
        },
        ValidationViolation::Unsupported {
            field: "request.input",
            rule: "supported modality",
        },
    ] {
        let error = KitError::new(
            ErrorKind::InvalidInput,
            Operation::Generate,
            ErrorPhase::InputValidation,
        )
        .with_native_details(NativeErrorDetails {
            message: Some("secret-caller-value".into()),
            ..Default::default()
        })
        .with_validation_violation(violation);
        assert_eq!(error.validation_violation(), Some(&violation));
        assert_eq!(error.clone().validation_violation(), Some(&violation));
        assert_eq!(error.context().operation, Operation::Generate);
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.to_string().contains("request.input"));
        assert!(!format!("{error:?}").contains("secret-caller-value"));
        assert!(!error.diagnostics_truncated());
    }
    let transport = KitError::new(
        ErrorKind::Transport,
        Operation::Configure,
        ErrorPhase::RequestBuild,
    );
    assert_eq!(transport.validation_violation(), None);
}

#[test]
fn diagnostic_resolution_cause_subtree_precedes_supplementary_capture() {
    let mut cause = KitError::new(
        ErrorKind::Transport,
        Operation::Generate,
        ErrorPhase::RequestSend,
    );
    for _ in 0..6 {
        cause = KitError::new(
            ErrorKind::Timeout,
            Operation::Generate,
            ErrorPhase::RequestSend,
        )
        .with_cause(cause);
    }
    let mut error = KitError::new(
        ErrorKind::Authentication,
        Operation::ListModels,
        ErrorPhase::ResponseHeaders,
    )
    .with_http_status(401)
    .with_cause(cause);
    for index in 0..30 {
        error = error.with_supplementary(
            KitError::new(
                ErrorKind::Transport,
                Operation::Shutdown,
                ErrorPhase::Cleanup,
            )
            .with_row(index),
        );
    }
    assert_eq!(
        error.supplementary().len(),
        24,
        "eight root/cause nodes take precedence"
    );
    assert_eq!(error.supplementary()[23].context().row, Some(23));
    assert_eq!(
        error.cause().unwrap().context().operation,
        Operation::Generate
    );
    assert_eq!(error.kind(), ErrorKind::Authentication);
    assert_eq!(error.http_status(), Some(401));
    assert!(error.diagnostics_truncated());
    assert!(std::error::Error::source(&error).is_some());
}

#[test]
fn diagnostic_resolution_response_and_native_strings_share_utf8_bounds() {
    let value = "é".repeat(40_000);
    let mut error = KitError::new(
        ErrorKind::InvalidResponse(ResponseViolation::UnknownVariant { field: "type" }),
        Operation::Generate,
        ErrorPhase::Decode,
    )
    .with_response_details(ResponseDetails::Discriminator {
        actual: value.clone(),
    })
    .with_request_target(value.clone())
    .with_correlation_id(value.clone());
    for _ in 0..20 {
        error = error.with_supplementary(
            KitError::new(
                ErrorKind::Transport,
                Operation::Generate,
                ErrorPhase::ReadBody,
            )
            .with_native_details(NativeErrorDetails {
                message: Some(value.clone()),
                ..Default::default()
            }),
        );
    }
    let Some(ResponseDetails::Discriminator { actual }) = error.response_details() else {
        panic!("discriminator retained")
    };
    let strings = std::iter::once(actual.as_str())
        .chain(error.request_target())
        .chain(error.correlation_id())
        .chain(
            error
                .supplementary()
                .iter()
                .filter_map(|child| child.native_details().message.as_deref()),
        );
    let mut total = 0;
    for text in strings {
        assert!(text.len() <= 65_536);
        assert!(text.chars().all(|value| value == 'é'));
        total += text.len();
    }
    assert!(total <= 1_048_576);
    assert!(error.diagnostics_truncated());
}

#[test]
fn diagnostic_resolution_tree_bounds_preserve_remote_failure_roundtrips() {
    for (kind, basis, details) in [
        (
            ErrorKind::Authentication,
            "native_code",
            NativeErrorDetails {
                code: Some("invalid_api_key".into()),
                ..Default::default()
            },
        ),
        (
            ErrorKind::Authentication,
            "native_error_type",
            NativeErrorDetails {
                error_type: Some("authentication_error".into()),
                ..Default::default()
            },
        ),
        (
            ErrorKind::Authentication,
            "canonical_status",
            NativeErrorDetails {
                native_status: Some("UNAUTHENTICATED".into()),
                ..Default::default()
            },
        ),
        (
            ErrorKind::Authentication,
            "native_reason",
            NativeErrorDetails {
                details: vec![NativeDetail::Reason {
                    domain: "fixture.service".into(),
                    reason: "API_KEY_INVALID".into(),
                }],
                ..Default::default()
            },
        ),
        (
            ErrorKind::ProviderRejected,
            "native_outcome",
            NativeErrorDetails {
                outcome: Some("OUTCOME_FAILED".into()),
                ..Default::default()
            },
        ),
    ] {
        let mut wire = serde_json::to_value(RemoteFailure::new(kind, details).unwrap()).unwrap();
        wire["basis"] = serde_json::json!({"source": basis});
        let remote: RemoteFailure = serde_json::from_value(wire).unwrap();
        for causal in [false, true] {
            let primary = KitError::new(
                ErrorKind::Transport,
                Operation::Generate,
                ErrorPhase::RequestSend,
            )
            .with_native_details(NativeErrorDetails {
                details: (0..16)
                    .map(|_| NativeDetail::QuotaViolation {
                        subject: None,
                        description: Some("x".repeat(65_536)),
                    })
                    .collect(),
                ..Default::default()
            });
            let child = KitError::from_remote(
                remote.clone(),
                Operation::Generate,
                ErrorPhase::ResponseHeaders,
            );
            let error = if causal {
                primary.with_cause(child)
            } else {
                primary.with_supplementary(child)
            };
            let child = if causal {
                error.cause()
            } else {
                error.supplementary().first()
            };
            if let Some(child) = child {
                let retained = child
                    .remote_failure()
                    .expect("retained remote classification");
                let restored: RemoteFailure =
                    serde_json::from_value(serde_json::to_value(retained).unwrap()).expect(
                        "every exposed remote failure still satisfies its deserialization contract",
                    );
                assert_eq!(&restored, retained);
                assert_eq!(
                    restored.classification_basis(),
                    remote.classification_basis()
                );
            }
            assert_eq!(error.kind(), ErrorKind::Transport);
            assert!(error.diagnostics_truncated());
        }
    }
}

#[test]
fn accepted_failed_row_round_trips_all_remote_details_and_redacts_debug() {
    let secret = "private-accepted-failure";
    let failure = RemoteFailure::new(
        ErrorKind::InvalidRequest,
        NativeErrorDetails {
            message: Some(secret.into()),
            body: Some(format!("{{\"native_extra\":\"{secret}\"}}")),
            code: Some("resource_invalid".into()),
            error_type: Some("invalid_request_error".into()),
            request_id: Some(secret.into()),
            reported_http_status: Some(400),
            ..Default::default()
        },
    )
    .unwrap();
    let expected_row =
        serde_json::json!({"status": "failed", "custom_id": "caller-row", "error": failure});
    let row: BatchResult = serde_json::from_value(expected_row.clone()).unwrap();
    assert_eq!(serde_json::to_value(&row).unwrap(), expected_row);
    let row_wire = serde_json::to_string(&row).unwrap();
    assert!(row_wire.contains(secret));
    assert_eq!(serde_json::from_str::<BatchResult>(&row_wire).unwrap(), row);
    assert!(!format!("{row:?}").contains(secret));
}

#[test]
fn accepted_failed_job_retains_every_error_and_redacts_debug() {
    let secret = "private-failed-job";
    let failure = RemoteFailure::new(
        ErrorKind::InvalidRequest,
        NativeErrorDetails {
            code: Some("invalid_request".into()),
            message: Some(secret.into()),
            request_id: Some(secret.into()),
            ..Default::default()
        },
    )
    .unwrap();
    let second = RemoteFailure::new(
        ErrorKind::NotFound,
        NativeErrorDetails {
            message: Some("second failure".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let expected_job = serde_json::json!({"status": "failed", "errors": [failure, second]});
    let job: BatchStatus = serde_json::from_value(expected_job.clone()).unwrap();
    assert_eq!(serde_json::to_value(&job).unwrap(), expected_job);
    let job_wire = serde_json::to_string(&job).unwrap();
    let restored = serde_json::from_str::<BatchStatus>(&job_wire).unwrap();
    assert_eq!(restored, job);
    assert_eq!(
        serde_json::to_value(&restored).unwrap()["errors"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(!format!("{job:?}").contains(secret));
}

#[test]
fn accepted_failed_hosted_output_round_trips_details_and_redacts_debug() {
    let secret = "private-hosted-failure";
    let failure = RemoteFailure::new(
        ErrorKind::InvalidRequest,
        NativeErrorDetails {
            code: Some("invalid_tool_input".into()),
            message: Some(secret.into()),
            body: Some(format!("{{\"native_extra\":\"{secret}\"}}")),
            ..Default::default()
        },
    )
    .unwrap();
    let expected_status = serde_json::json!({"state": "failed", "error": failure});
    let status: HostedToolStatus = serde_json::from_value(expected_status.clone()).unwrap();
    assert_eq!(serde_json::to_value(&status).unwrap(), expected_status);
    let hosted = HostedToolOutput::CodeExecution {
        status: Some(status),
        code: Vec::new(),
        results: Vec::new(),
        artifacts: Vec::new(),
    };
    let hosted_wire = serde_json::to_string(&hosted).unwrap();
    assert!(hosted_wire.contains(secret));
    assert_eq!(
        serde_json::from_str::<HostedToolOutput>(&hosted_wire).unwrap(),
        hosted
    );
    assert!(!format!("{hosted:?}").contains(secret));
}

#[test]
fn failed_job_without_native_diagnostics_does_not_invent_an_error() {
    let expected = serde_json::json!({"status": "failed", "errors": []});
    let job: BatchStatus = serde_json::from_value(expected.clone()).unwrap();
    assert_eq!(serde_json::to_value(&job).unwrap(), expected);
    let wire = serde_json::to_string(&job).unwrap();
    assert_eq!(serde_json::from_str::<BatchStatus>(&wire).unwrap(), job);
}

#[test]
fn native_execution_exit_alone_can_support_an_outcome_classification() {
    let failure = RemoteFailure::new(
        ErrorKind::ProviderRejected,
        NativeErrorDetails {
            execution_exit_code: Some(7),
            ..Default::default()
        },
    )
    .unwrap();
    let mut wire = serde_json::to_value(failure).unwrap();
    wire["basis"] = serde_json::json!({ "source": "native_outcome" });
    let restored: RemoteFailure = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(
        restored.classification_basis(),
        ClassificationBasis::NativeOutcome
    );
    assert_eq!(restored.native_details().outcome, None);
    assert_eq!(restored.native_details().execution_exit_code, Some(7));
    wire["details"]["execution_exit_code"] = serde_json::json!(0);
    assert!(serde_json::from_value::<RemoteFailure>(wire).is_err());
}

#[test]
fn transport_status_basis_and_native_reported_status_remain_independent() {
    let failure = RemoteFailure::new(
        ErrorKind::Authentication,
        NativeErrorDetails {
            reported_http_status: Some(404),
            code: Some("model_not_found".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let mut wire = serde_json::to_value(failure).unwrap();
    wire["basis"] = serde_json::json!({"source": "http_status", "status": 401});
    let failure: RemoteFailure = serde_json::from_value(wire).unwrap();
    let error = KitError::from_remote(failure, Operation::Generate, ErrorPhase::ResponseHeaders)
        .with_http_status(401);
    assert_eq!(error.kind(), ErrorKind::Authentication);
    assert_eq!(
        error.classification_basis(),
        ClassificationBasis::HttpStatus { status: 401 }
    );
    assert_eq!(error.http_status(), Some(401));
    assert_eq!(error.native_details().reported_http_status, Some(404));
    assert_eq!(
        error.native_details().code.as_deref(),
        Some("model_not_found")
    );
    let error = error.with_native_details(NativeErrorDetails {
        message: Some("caller replacement".into()),
        ..Default::default()
    });
    assert!(error.remote_failure().is_some());
    assert_eq!(error.classification_basis(), ClassificationBasis::Explicit);
    assert_eq!(
        error.remote_failure().unwrap().native_details(),
        error.native_details()
    );
}

#[test]
fn bounded_capture_preserves_first_reason_and_reports_omitted_records() {
    let mut details: Vec<NativeDetail> = (0..256)
        .map(|_| NativeDetail::RetryDelay {
            seconds: 1,
            nanos: 0,
        })
        .collect();
    details.push(NativeDetail::Reason {
        domain: "googleapis.com".into(),
        reason: "API_KEY_INVALID".into(),
    });
    let error = KitError::new(
        ErrorKind::Authentication,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
    )
    .with_native_details(NativeErrorDetails {
        details,
        ..Default::default()
    });
    assert_eq!(error.native_details().details.len(), 256);
    assert_eq!(error.native_details().omitted_details, 1);
    assert!(error.native_details().truncated);
    assert!(error.native_details().details.iter().any(|detail| matches!(detail, NativeDetail::Reason { domain, reason } if domain == "googleapis.com" && reason == "API_KEY_INVALID")));
}

#[test]
fn aggregate_capture_budget_includes_response_observations_and_local_identity() {
    let error = KitError::new(
        ErrorKind::InvalidResponse(ResponseViolation::IdentityMismatch { field: "model" }),
        Operation::Generate,
        ErrorPhase::Normalize,
    )
    .with_native_details(NativeErrorDetails {
        details: (0..16)
            .map(|_| NativeDetail::Reason {
                domain: "a".repeat(65_536),
                reason: "b".repeat(65_536),
            })
            .collect(),
        ..Default::default()
    })
    .with_response_details(ResponseDetails::Identity {
        expected: "c".repeat(65_536),
        actual: "d".repeat(65_536),
    })
    .with_request_target("e".repeat(65_536))
    .with_correlation_id("f".repeat(65_536));
    let native_bytes: usize = error
        .native_details()
        .details
        .iter()
        .map(|detail| match detail {
            NativeDetail::Reason { domain, reason } => domain.len() + reason.len(),
            _ => 0,
        })
        .sum();
    let response_bytes = match error.response_details().unwrap() {
        ResponseDetails::Identity { expected, actual } => expected.len() + actual.len(),
        _ => panic!("identity observations disappeared"),
    };
    let total = native_bytes
        + response_bytes
        + error.request_target().unwrap().len()
        + error.correlation_id().unwrap().len();
    assert!(total <= 1_048_576);
    assert!(error.native_details().truncated);
}

#[test]
fn remote_failure_admits_service_categories_without_claiming_native_evidence() {
    for kind in [
        ErrorKind::Unsupported,
        ErrorKind::InvalidRequest,
        ErrorKind::Authentication,
        ErrorKind::PermissionDenied,
        ErrorKind::NotFound,
        ErrorKind::Conflict,
        ErrorKind::RequestTooLarge,
        ErrorKind::FailedPrecondition,
        ErrorKind::ResourceExhausted,
        ErrorKind::RateLimited,
        ErrorKind::QuotaExceeded,
        ErrorKind::Billing,
        ErrorKind::ServiceUnavailable,
        ErrorKind::Internal,
        ErrorKind::DeadlineExceeded,
        ErrorKind::Cancelled,
        ErrorKind::ProviderRejected,
    ] {
        let failure = RemoteFailure::new(kind, NativeErrorDetails::default()).unwrap();
        assert_eq!(failure.kind(), kind);
        assert_eq!(
            failure.classification_basis(),
            ClassificationBasis::Explicit
        );
        assert_eq!(failure.native_details(), &NativeErrorDetails::default());
    }
}

#[test]
fn remote_failure_rejects_runtime_and_response_schema_categories() {
    for kind in [
        ErrorKind::Configuration,
        ErrorKind::InvalidInput,
        ErrorKind::Transport,
        ErrorKind::Timeout,
        ErrorKind::Closed,
        ErrorKind::InvalidState,
        ErrorKind::InvalidResponse(ResponseViolation::MalformedJson),
    ] {
        let error = RemoteFailure::new(kind, NativeErrorDetails::default()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn remote_failure_round_trip_retains_independent_native_facts_without_printing_them() {
    let secret = "private-error-evidence";
    let failure = RemoteFailure::new(
        ErrorKind::NotFound,
        NativeErrorDetails {
            message: Some(secret.into()),
            body: Some(format!("{{\"unknown_native_field\":\"{secret}\"}}")),
            code: Some("unknown_service_code".into()),
            error_type: Some("inner_error_type".into()),
            event_type: Some("error".into()),
            outcome: Some("OUTCOME_FAILED".into()),
            parameter: Some("model".into()),
            request_id: Some(secret.into()),
            event_id: Some("server-event".into()),
            related_event_id: Some("client-event".into()),
            tool_call_id: Some(secret.into()),
            sequence_number: Some(73),
            native_status: Some("NOT_FOUND".into()),
            reported_http_status: Some(404),
            rpc_code: Some(5),
            execution_exit_code: Some(5),
            retry_after: Some("1".into()),
            url: Some(secret.into()),
            details: vec![
                NativeDetail::Reason {
                    domain: "example.invalid".into(),
                    reason: "UNKNOWN_REASON".into(),
                },
                NativeDetail::Resource {
                    resource_type: Some("file".into()),
                    name: secret.into(),
                    owner: None,
                    description: None,
                },
                NativeDetail::FieldViolation {
                    field: "input.file".into(),
                    reason: Some("INVALID".into()),
                    description: Some(secret.into()),
                },
                NativeDetail::RetryDelay {
                    seconds: 1,
                    nanos: 2,
                },
                NativeDetail::QuotaViolation {
                    subject: Some(secret.into()),
                    description: None,
                },
                NativeDetail::PreconditionViolation {
                    kind: Some("TOS".into()),
                    subject: None,
                    description: Some(secret.into()),
                },
            ],
            truncated: true,
            omitted_details: 2,
        },
    )
    .unwrap();
    let serialized = serde_json::to_string(&failure).unwrap();
    assert!(
        serialized.contains(secret),
        "explicit serialization preserves retained evidence"
    );
    let restored: RemoteFailure = serde_json::from_str(&serialized).unwrap();
    assert_eq!(restored, failure);
    assert!(!format!("{failure:?}").contains(secret));
    assert!(!format!("{failure}").contains(secret));
    assert!(!format!("{:?}", failure.native_details()).contains(secret));
    assert!(!format!("{:?}", failure.native_details().details).contains(secret));
}

#[test]
fn remote_failure_rejects_oversized_caller_details_instead_of_losing_evidence() {
    let too_long = NativeErrorDetails {
        body: Some("é".repeat(32_769)),
        ..NativeErrorDetails::default()
    };
    let too_many = NativeErrorDetails {
        details: (0..257)
            .map(|_| NativeDetail::RetryDelay {
                seconds: 1,
                nanos: 0,
            })
            .collect(),
        ..NativeErrorDetails::default()
    };
    let too_large = NativeErrorDetails {
        details: (0..9)
            .map(|_| NativeDetail::Reason {
                domain: "a".repeat(65_536),
                reason: "b".repeat(65_536),
            })
            .collect(),
        ..NativeErrorDetails::default()
    };
    let oversized_tool_correlation = NativeErrorDetails {
        tool_call_id: Some("x".repeat(65_537)),
        ..Default::default()
    };
    for details in [too_long, too_many, too_large, oversized_tool_correlation] {
        let error = RemoteFailure::new(ErrorKind::ProviderRejected, details).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn remote_failure_deserialization_rejects_invalid_categories_basis_and_bounds() {
    let failure = RemoteFailure::new(ErrorKind::NotFound, NativeErrorDetails::default()).unwrap();
    let valid = serde_json::to_value(&failure).unwrap();
    let mut invalid_kind = valid.clone();
    invalid_kind["kind"] = serde_json::json!("transport");
    let mut unsupported_basis = valid.clone();
    unsupported_basis["basis"] = serde_json::json!({"source": "native_code"});
    let mut oversized = valid.clone();
    oversized["details"]["body"] = serde_json::json!("x".repeat(65_537));
    let mut unknown = valid;
    unknown["extra_error"] = serde_json::json!({"kind": "authentication"});
    for invalid in [invalid_kind, unsupported_basis, oversized, unknown] {
        assert!(serde_json::from_value::<RemoteFailure>(invalid).is_err());
    }
}

#[test]
fn causal_failures_remain_separate_from_cleanup_and_remote_values() {
    use std::error::Error as _;

    let remote = RemoteFailure::new(
        ErrorKind::Authentication,
        NativeErrorDetails {
            code: Some("private-native-code".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let remote_error = KitError::from_remote(
        remote.clone(),
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
    )
    .with_http_status(401);
    assert_eq!(remote_error.remote_failure(), Some(&remote));
    assert_eq!(
        remote_error
            .source()
            .unwrap()
            .downcast_ref::<RemoteFailure>(),
        Some(&remote)
    );
    let error = KitError::new(
        ErrorKind::InvalidState,
        Operation::Generate,
        ErrorPhase::Lifecycle,
    )
    .with_cause(remote_error)
    .with_supplementary(KitError::new(
        ErrorKind::Transport,
        Operation::DeleteFile,
        ErrorPhase::Cleanup,
    ));
    let cause = error.source().unwrap().downcast_ref::<KitError>().unwrap();
    assert_eq!(cause.kind(), ErrorKind::Authentication);
    assert_eq!(error.cause().unwrap().http_status(), Some(401));
    assert_eq!(
        error.supplementary()[0].context().operation,
        Operation::DeleteFile
    );
    assert_eq!(
        error.supplementary()[0].context().phase,
        ErrorPhase::Cleanup
    );
    for formatted in [
        format!("{error:?}"),
        format!("{}", error.source().unwrap()),
        format!("{:?}", error.source().unwrap().source().unwrap()),
    ] {
        assert!(!formatted.contains("private-native-code"));
    }
}

#[test]
fn response_observations_and_local_identity_do_not_become_native_attribution() {
    let secret = "private-model-and-discriminator";
    let error = KitError::new(
        ErrorKind::InvalidResponse(ResponseViolation::UnknownVariant { field: "type" }),
        Operation::BatchResults,
        ErrorPhase::Normalize,
    )
    .with_response_details(ResponseDetails::Discriminator {
        actual: secret.into(),
    })
    .with_request_target(secret)
    .with_correlation_id("row-custom-id");
    assert_eq!(error.request_target(), Some(secret));
    assert_eq!(error.correlation_id(), Some("row-custom-id"));
    assert_eq!(
        error.response_details(),
        Some(&ResponseDetails::Discriminator {
            actual: secret.into()
        })
    );
    assert!(error.native_details().details.is_empty());
    assert_eq!(error.classification_basis(), ClassificationBasis::Explicit);
    assert!(!format!("{error:?}").contains(secret));
    assert!(!format!("{:?}", error.response_details()).contains(secret));
    assert!(error.to_string().contains("UnknownVariant"));
}

#[test]
fn structured_response_reason_is_reportable_without_native_secrets() {
    let secret = "private-provider-secret";
    let error = KitError::new(
        ErrorKind::InvalidResponse(ResponseViolation::MissingField {
            field: "modelVersion",
        }),
        Operation::BatchResults,
        ErrorPhase::Normalize,
    )
    .with_row(2)
    .with_native_details(NativeErrorDetails {
        message: Some(secret.into()),
        body: Some(secret.into()),
        code: Some(secret.into()),
        error_type: Some(secret.into()),
        parameter: Some(secret.into()),
        request_id: Some(secret.into()),
        event_id: Some(secret.into()),
        related_event_id: Some(secret.into()),
        tool_call_id: Some(secret.into()),
        sequence_number: Some(91),
        native_status: Some(secret.into()),
        retry_after: Some(secret.into()),
        url: Some(secret.into()),
        details: vec![NativeDetail::Resource {
            resource_type: None,
            name: secret.into(),
            owner: None,
            description: None,
        }],
        ..NativeErrorDetails::default()
    })
    .with_supplementary(
        KitError::new(
            ErrorKind::Transport,
            Operation::DeleteFile,
            ErrorPhase::Cleanup,
        )
        .with_native_details(NativeErrorDetails {
            message: Some(secret.into()),
            ..NativeErrorDetails::default()
        }),
    );
    for formatted in [
        format!("{error}"),
        format!("{error:?}"),
        format!("{:?}", error.native_details()),
    ] {
        assert!(!formatted.contains(secret));
    }
    let formatted = error.to_string();
    assert!(formatted.contains("MissingField"));
    assert!(formatted.contains("modelVersion"));
    assert!(formatted.contains("BatchResults"));
    assert!(formatted.contains("Normalize"));
    assert_eq!(error.context().row, Some(2));
    assert_eq!(error.native_details().code.as_deref(), Some(secret));
    assert_eq!(
        error.clone().native_details().request_id.as_deref(),
        Some(secret)
    );
}

#[test]
fn supplementary_failure_does_not_replace_primary_classification() {
    let secondary = KitError::new(
        ErrorKind::Transport,
        Operation::Generate,
        ErrorPhase::ReadBody,
    );
    let primary = KitError::new(
        ErrorKind::Authentication,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
    )
    .with_http_status(401)
    .with_supplementary(secondary);
    assert_eq!(primary.kind(), ErrorKind::Authentication);
    assert_eq!(primary.http_status(), Some(401));
    assert_eq!(primary.supplementary()[0].kind(), ErrorKind::Transport);
    assert_eq!(
        primary.supplementary()[0].context().phase,
        ErrorPhase::ReadBody
    );
}

#[test]
fn streamed_remote_error_does_not_invent_http_status() {
    let error = KitError::new(
        ErrorKind::NotFound,
        Operation::GenerateStream,
        ErrorPhase::Stream,
    )
    .with_native_details(NativeErrorDetails {
        code: Some("resource_not_found".into()),
        ..NativeErrorDetails::default()
    });
    assert_eq!(error.http_status(), None);
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert_eq!(error.context().phase, ErrorPhase::Stream);
}

#[test]
fn native_details_are_bounded_on_public_construction() {
    let error = KitError::new(
        ErrorKind::ProviderRejected,
        Operation::Generate,
        ErrorPhase::ResponseHeaders,
    )
    .with_native_details(NativeErrorDetails {
        body: Some("é".repeat(100_000)),
        ..NativeErrorDetails::default()
    });
    assert!(error.native_details().body.as_ref().unwrap().len() <= 65_536);
    assert!(error.native_details().truncated);
}

#[test]
fn remote_serde_rejects_known_success_and_contradictory_standardized_basis() {
    let cases = [
        serde_json::json!({"kind":"provider_rejected","basis":{"source":"rpc_code"},"details":{"rpc_code":0}}),
        serde_json::json!({"kind":"provider_rejected","basis":{"source":"canonical_status"},"details":{"native_status":"OK"}}),
        serde_json::json!({"kind":"provider_rejected","basis":{"source":"native_outcome"},"details":{"outcome":"OUTCOME_OK","execution_exit_code":0}}),
        serde_json::json!({"kind":"not_found","basis":{"source":"http_status","status":401},"details":{}}),
        serde_json::json!({"kind":"authentication","basis":{"source":"rpc_code"},"details":{"rpc_code":5}}),
        serde_json::json!({"kind":"authentication","basis":{"source":"canonical_status"},"details":{"native_status":"NOT_FOUND"}}),
    ];
    for wire in cases {
        assert!(
            serde_json::from_value::<RemoteFailure>(wire.clone()).is_err(),
            "accepted contradictory basis: {wire}"
        );
    }
}

#[test]
fn remote_serde_preserves_native_unimplemented_as_unsupported() {
    for wire in [
        serde_json::json!({"kind":"unsupported","basis":{"source":"rpc_code"},"details":{"rpc_code":12}}),
        serde_json::json!({"kind":"unsupported","basis":{"source":"canonical_status"},"details":{"native_status":"UNIMPLEMENTED"}}),
    ] {
        let failure: RemoteFailure = serde_json::from_value(wire).unwrap();
        assert_eq!(failure.kind(), ErrorKind::Unsupported);
        let restored: RemoteFailure =
            serde_json::from_str(&serde_json::to_string(&failure).unwrap()).unwrap();
        assert_eq!(restored, failure);
    }
}

#[test]
fn remote_serde_unknown_standardized_values_require_unclassified_basis() {
    for (basis, details) in [
        ("rpc_code", serde_json::json!({"rpc_code":-1})),
        ("rpc_code", serde_json::json!({"rpc_code":17})),
        ("rpc_code", serde_json::json!({"rpc_code":999})),
        (
            "canonical_status",
            serde_json::json!({"native_status":"FUTURE_NATIVE_STATUS"}),
        ),
    ] {
        let expected: NativeErrorDetails = serde_json::from_value(details.clone()).unwrap();
        let wire = serde_json::json!({"kind":"provider_rejected","basis":{"source":basis},"details":details});
        assert!(
            serde_json::from_value::<RemoteFailure>(wire.clone()).is_err(),
            "unknown value cannot claim recognized standardized basis: {wire}"
        );
        let mut unclassified = wire;
        unclassified["basis"] = serde_json::json!({"source":"unclassified"});
        let failure: RemoteFailure = serde_json::from_value(unclassified.clone()).unwrap();
        assert_eq!(failure.kind(), ErrorKind::ProviderRejected);
        assert_eq!(failure.native_details(), &expected);
        assert_eq!(
            serde_json::from_str::<RemoteFailure>(&serde_json::to_string(&failure).unwrap())
                .unwrap(),
            failure
        );
    }
}

#[test]
fn remote_serde_known_execution_outcomes_cannot_claim_unrelated_categories() {
    for (kind, details) in [
        (
            "authentication",
            serde_json::json!({"outcome":"OUTCOME_FAILED"}),
        ),
        (
            "provider_rejected",
            serde_json::json!({"outcome":"OUTCOME_DEADLINE_EXCEEDED"}),
        ),
        (
            "provider_rejected",
            serde_json::json!({"outcome":"URL_RETRIEVAL_STATUS_PAYWALL"}),
        ),
        (
            "not_found",
            serde_json::json!({"outcome":"URL_RETRIEVAL_STATUS_ERROR"}),
        ),
        (
            "authentication",
            serde_json::json!({"execution_exit_code":1}),
        ),
    ] {
        let wire =
            serde_json::json!({"kind":kind,"basis":{"source":"native_outcome"},"details":details});
        assert!(
            serde_json::from_value::<RemoteFailure>(wire.clone()).is_err(),
            "native outcome contradicts category: {wire}"
        );
    }
}

#[test]
fn remote_serde_preserves_supported_execution_outcome_classifications() {
    for (kind, details) in [
        (
            "provider_rejected",
            serde_json::json!({"outcome":"OUTCOME_FAILED"}),
        ),
        (
            "deadline_exceeded",
            serde_json::json!({"outcome":"OUTCOME_DEADLINE_EXCEEDED"}),
        ),
        (
            "permission_denied",
            serde_json::json!({"outcome":"URL_RETRIEVAL_STATUS_PAYWALL"}),
        ),
        (
            "permission_denied",
            serde_json::json!({"outcome":"URL_RETRIEVAL_STATUS_UNSAFE"}),
        ),
        (
            "provider_rejected",
            serde_json::json!({"outcome":"URL_RETRIEVAL_STATUS_ERROR"}),
        ),
        (
            "provider_rejected",
            serde_json::json!({"execution_exit_code":1}),
        ),
        (
            "provider_rejected",
            serde_json::json!({"execution_exit_code":-1}),
        ),
    ] {
        let wire =
            serde_json::json!({"kind":kind,"basis":{"source":"native_outcome"},"details":details});
        let failure: RemoteFailure = serde_json::from_value(wire).unwrap();
        assert_eq!(
            serde_json::from_str::<RemoteFailure>(&serde_json::to_string(&failure).unwrap())
                .unwrap(),
            failure
        );
    }
}
