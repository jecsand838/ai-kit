use providers_kit::KitError;
use providers_kit::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, NativeDetail, NativeErrorDetails, Operation,
    RemoteFailure, ResponseViolation,
};
use providers_kit::generation::{
    Artifact, Citation, ContentPart, FileReference, FinishReason, FunctionCall, GenerateEvent,
    GenerateResponse, Message, Usage,
};
use providers_kit::hosted::{
    HostedCodeExecutionResult, HostedExecutableCode, HostedFileSearchResult, HostedToolOutput,
    HostedToolStatus, HostedWebSearchAction, HostedWebSearchResult,
};
use providers_kit::models::ModelId;

use crate::hosted_assertions::{FileSearchContract, HostedAnswer, HostedEvidence, HostedProvider};
use crate::live_assertions::{
    FileSearchCapture, FileSearchStage, GenerationEvidence, StreamEvidence,
};
use crate::live_failures::CaseFailure;

struct HostedFixture;

#[test]
fn reaudit_positive_hosted_checks_reject_failures_from_other_families() {
    let web = HostedFixture::web(Some(HostedToolStatus::Completed));
    let url = HostedToolOutput::UrlContext {
        requested_url: None,
        retrieved_url: Some(HostedFixture::URL.into()),
        status: Some(HostedToolStatus::Completed),
        citations: Vec::new(),
    };
    let code = HostedFixture::paired(
        HostedProvider::Gemini,
        HostedFixture::code(None),
        HostedFixture::result(None, Some(HostedToolStatus::Completed), "338350\n", None),
    );
    for valid in [&web, &url, &code] {
        let response = HostedFixture::response(vec![valid.clone()]);
        match valid {
            HostedToolOutput::WebSearch { .. } => {
                HostedEvidence::web_search(&response, HostedProvider::OpenAi)
            }
            HostedToolOutput::UrlContext { .. } => {
                HostedEvidence::url_context(&response, HostedProvider::Gemini, HostedFixture::URL)
            }
            HostedToolOutput::CodeExecution { .. } => {
                HostedEvidence::code_success(&response, HostedProvider::Gemini, "338350")
            }
            HostedToolOutput::FileSearch { .. } => unreachable!(),
        }
        .expect("positive control must independently prove the requested operation");
        for status in [HostedFixture::failure(), HostedToolStatus::Incomplete] {
            let mut failed_file = HostedFixture::file();
            if let HostedToolOutput::FileSearch {
                status: file_status,
                ..
            } = &mut failed_file
            {
                *file_status = Some(status.clone());
            }
            for failed in [
                HostedFixture::web(Some(status.clone())),
                failed_file,
                HostedToolOutput::UrlContext {
                    requested_url: None,
                    retrieved_url: None,
                    status: Some(status.clone()),
                    citations: Vec::new(),
                },
                HostedFixture::code(Some(status.clone())),
                HostedFixture::result(None, Some(status), "", None),
                HostedFixture::result(None, None, "", Some(1)),
            ] {
                if std::mem::discriminant(valid) == std::mem::discriminant(&failed) {
                    continue;
                }
                for outputs in [
                    vec![valid.clone(), failed.clone()],
                    vec![failed, valid.clone()],
                ] {
                    let response = HostedFixture::response(outputs);
                    let result = match valid {
                        HostedToolOutput::WebSearch { .. } => {
                            HostedEvidence::web_search(&response, HostedProvider::OpenAi)
                        }
                        HostedToolOutput::UrlContext { .. } => HostedEvidence::url_context(
                            &response,
                            HostedProvider::Gemini,
                            HostedFixture::URL,
                        ),
                        HostedToolOutput::CodeExecution { .. } => HostedEvidence::code_success(
                            &response,
                            HostedProvider::Gemini,
                            "338350",
                        ),
                        HostedToolOutput::FileSearch { .. } => unreachable!(),
                    };
                    assert!(
                        result.is_err(),
                        "positive evidence must not hide another hosted operation's terminal failure: {response:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn reaudit_stream_cannot_replace_incomplete_hosted_execution_with_success() {
    for (observed, should_fail) in [
        (
            HostedFixture::code(Some(HostedToolStatus::Incomplete)),
            true,
        ),
        (
            HostedFixture::result(None, Some(HostedToolStatus::Incomplete), "", None),
            true,
        ),
        (
            HostedFixture::code(Some(HostedToolStatus::InProgress)),
            false,
        ),
    ] {
        let mut response =
            HostedFixture::response(vec![HostedFixture::code(Some(HostedToolStatus::Completed))]);
        response.message = Message::assistant_text("answer").unwrap();
        let mut evidence = StreamEvidence::new();
        evidence
            .observe(GenerateEvent::Started {
                model: response.model.clone(),
            })
            .unwrap();
        evidence
            .observe(GenerateEvent::ContentDelta {
                delta: "answer".into(),
            })
            .unwrap();
        evidence
            .observe(GenerateEvent::HostedTool(Box::new(observed)))
            .unwrap();
        assert_eq!(
            evidence
                .observe(GenerateEvent::Completed(Box::new(response)))
                .is_err(),
            should_fail,
            "terminal incompleteness must survive; ordinary progress may settle successfully"
        );
    }
}

#[test]
fn openai_child_exception_requires_actual_paired_logs() {
    let source = "import subprocess, sys\nresult = subprocess.run([sys.executable, \"-c\", 'raise RuntimeError(\"EXPECTED_SENTINEL\")'], capture_output=True, text=True, timeout=10)\nprint(result.stderr, end=\"\")\nresult.check_returncode()";
    let mut output = HostedFixture::openai_failure_record(
        "Traceback (most recent call last):\n\u{1b}[1;35mRuntimeError\u{1b}[0m: \u{1b}[35mEXPECTED_SENTINEL\u{1b}[0m\n",
    );
    if let HostedToolOutput::CodeExecution { code, .. } = &mut output {
        code[0].code = source.into();
    }
    HostedEvidence::code_failure(
        &HostedFixture::response(vec![output.clone()]),
        HostedProvider::OpenAi,
        "EXPECTED_SENTINEL",
    )
    .expect("real child stderr and its exact execution source prove the exception");
    for logs in ["", "RuntimeError: DIFFERENT_SENTINEL\n"] {
        let mut bad = output.clone();
        if let HostedToolOutput::CodeExecution { results, .. } = &mut bad {
            results[0].logs = Some(logs.into());
        }
        assert!(
            HostedEvidence::code_failure(
                &HostedFixture::response(vec![bad]),
                HostedProvider::OpenAi,
                "EXPECTED_SENTINEL"
            )
            .is_err()
        );
    }
    if let HostedToolOutput::CodeExecution { results, .. } = &mut output {
        results.clear();
    }
    assert!(
        HostedEvidence::code_failure(
            &HostedFixture::response(vec![output]),
            HostedProvider::OpenAi,
            "EXPECTED_SENTINEL"
        )
        .is_err()
    );
}

#[test]
fn openai_child_exception_rejects_an_additional_execution() {
    let valid = HostedFixture::openai_failure_record("RuntimeError: EXPECTED_SENTINEL\n");
    let mut extra = HostedFixture::openai_failure_record("1\n");
    let HostedToolOutput::CodeExecution { code, .. } = &mut extra else {
        panic!("interpreter fixture");
    };
    code[0].code = "print(1)".into();
    for outputs in [vec![valid.clone(), extra.clone()], vec![extra, valid]] {
        assert!(
            HostedEvidence::code_failure(
                &HostedFixture::response(outputs),
                HostedProvider::OpenAi,
                "EXPECTED_SENTINEL",
            )
            .is_err(),
            "a valid exception cannot authorize an additional execution"
        );
    }
}

#[test]
fn openai_child_exception_rejects_duplicate_valid_executions() {
    let valid = HostedFixture::openai_failure_record("RuntimeError: EXPECTED_SENTINEL\n");
    assert!(
        HostedEvidence::code_failure(
            &HostedFixture::response(vec![valid.clone(), valid]),
            HostedProvider::OpenAi,
            "EXPECTED_SENTINEL",
        )
        .is_err(),
        "the requested exception must execute exactly once"
    );
}

#[test]
fn openai_child_exception_rejects_an_additional_execution_without_logs() {
    let valid = HostedFixture::openai_failure_record("RuntimeError: EXPECTED_SENTINEL\n");
    let mut extra = HostedFixture::openai_failure_record("");
    let HostedToolOutput::CodeExecution { code, results, .. } = &mut extra else {
        panic!("interpreter fixture");
    };
    code[0].code = "print(1)".into();
    results.clear();
    assert!(
        HostedEvidence::code_failure(
            &HostedFixture::response(vec![valid, extra]),
            HostedProvider::OpenAi,
            "EXPECTED_SENTINEL",
        )
        .is_err(),
        "a completed extra interpreter call still counts when outputs are unavailable"
    );
}

#[test]
fn openai_child_exception_accepts_multiple_logs_from_one_execution() {
    let mut output = HostedFixture::openai_failure_record("Traceback (most recent call last):\n");
    let HostedToolOutput::CodeExecution { results, .. } = &mut output else {
        panic!("interpreter fixture");
    };
    results.push(HostedCodeExecutionResult {
        status: None,
        logs: Some("RuntimeError: EXPECTED_SENTINEL\n".into()),
        stdout: None,
        stderr: None,
        exit_code: None,
    });
    HostedEvidence::code_failure(
        &HostedFixture::response(vec![output]),
        HostedProvider::OpenAi,
        "EXPECTED_SENTINEL",
    )
    .expect("multiple native log outputs still belong to one interpreter execution");
}

impl HostedFixture {
    fn captured_stream(directory: &std::path::Path, stage: FileSearchStage) -> StreamEvidence {
        StreamEvidence::new().with_file_search_capture(FileSearchCapture {
            contract: FileSearchContract::GeminiStreaming,
            selected_model: "models/gemini-2.5-flash".into(),
            stage,
            directory: directory.into(),
            secrets: vec!["private-capture-credential".into()],
        })
    }

    fn captures(directory: &std::path::Path) -> Vec<serde_json::Value> {
        let mut captures = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    assert_eq!(
                        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                        0o600
                    );
                }
                let saved: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
                assert_eq!(saved["truncated"], false);
                saved["observation"].clone()
            })
            .collect::<Vec<_>>();
        captures.sort_by_key(|entry| entry["completion_ordinal"].as_u64().unwrap());
        captures
    }

    const URL: &str = "https://www.iana.org/help/example-domains";
    const MISSING_OPENAI_STORE: &str = "vs_strict-0123456789abcdef0123456789abcdef";

    fn response(outputs: Vec<HostedToolOutput>) -> GenerateResponse {
        GenerateResponse {
            model: ModelId::new("assertion-fixture").unwrap(),
            message: Message::assistant_text("RuntimeError: EXPECTED_SENTINEL").unwrap(),
            finish_reason: Some(FinishReason::Stop),
            citations: Vec::new(),
            artifacts: Vec::new(),
            hosted_tool_outputs: outputs,
            usage: Usage::default(),
        }
    }

    fn web(status: Option<HostedToolStatus>) -> HostedToolOutput {
        HostedToolOutput::WebSearch {
            status,
            action: Some(HostedWebSearchAction::Search {
                query: Some("IANA example domains".into()),
                queries: Vec::new(),
                sources: Vec::new(),
            }),
            results: vec![HostedWebSearchResult {
                title: Some("Example Domains".into()),
                url: Self::URL.into(),
                snippet: None,
            }],
            citations: vec![Citation::Url {
                url: Self::URL.into(),
                title: Some("Example Domains".into()),
                snippet: None,
                start_index: None,
                end_index: None,
            }],
        }
    }

    fn code(status: Option<HostedToolStatus>) -> HostedToolOutput {
        HostedToolOutput::CodeExecution {
            status,
            code: vec![HostedExecutableCode {
                language: Some("PYTHON".into()),
                code: "print(sum(i*i for i in range(1, 101)))".into(),
            }],
            results: Vec::new(),
            artifacts: Vec::new(),
        }
    }

    fn result(
        status: Option<HostedToolStatus>,
        result_status: Option<HostedToolStatus>,
        stdout: &str,
        exit_code: Option<i64>,
    ) -> HostedToolOutput {
        let successful = matches!(result_status, Some(HostedToolStatus::Completed));
        HostedToolOutput::CodeExecution {
            status,
            code: Vec::new(),
            results: vec![HostedCodeExecutionResult {
                status: result_status,
                logs: (!successful).then(|| stdout.into()),
                stdout: successful.then(|| stdout.into()),
                stderr: None,
                exit_code,
            }],
            artifacts: Vec::new(),
        }
    }

    fn paired(
        provider: HostedProvider,
        source: HostedToolOutput,
        mut output: HostedToolOutput,
    ) -> HostedToolOutput {
        let HostedToolOutput::CodeExecution { code: source, .. } = source else {
            panic!("execution fixture source");
        };
        let HostedToolOutput::CodeExecution { status, code, .. } = &mut output else {
            panic!("execution fixture result");
        };
        *code = source;
        if matches!(provider, HostedProvider::Gemini) {
            *status = None;
        }
        output
    }

    fn stderr(mut output: HostedToolOutput) -> HostedToolOutput {
        if let HostedToolOutput::CodeExecution { results, .. } = &mut output {
            for result in results {
                result.stderr = result.logs.take().or_else(|| result.stdout.take());
            }
        }
        output
    }

    fn failure() -> HostedToolStatus {
        HostedToolStatus::Failed {
            error: RemoteFailure::new(
                ErrorKind::ProviderRejected,
                NativeErrorDetails {
                    outcome: Some("OUTCOME_FAILED".into()),
                    ..NativeErrorDetails::default()
                },
            )
            .unwrap(),
        }
    }

    fn file() -> HostedToolOutput {
        HostedToolOutput::FileSearch {
            status: Some(HostedToolStatus::Completed),
            queries: vec!["fixture marker".into()],
            results: Some(vec![HostedFileSearchResult {
                file_id: "file-real-fixture".into(),
                file_name: None,
                text: Some("ai-kit-provider-strict-file-search-v1".into()),
                score: None,
            }]),
            citations: vec![Citation::File {
                reference: FileReference::Provider {
                    id: "file-real-fixture".into(),
                    media_type: None,
                },
                title: None,
                snippet: None,
                start_index: None,
                end_index: None,
            }],
        }
    }

    fn gemini_file_search_with_query(source: &str) -> GenerateResponse {
        let mut response = Self::response(vec![
            HostedToolOutput::CodeExecution {
                status: None,
                code: vec![HostedExecutableCode {
                    language: Some("PYTHON".into()),
                    code: source.into(),
                }],
                results: Vec::new(),
                artifacts: Vec::new(),
            },
            HostedToolOutput::FileSearch {
                status: None,
                queries: Vec::new(),
                results: Some(Vec::new()),
                citations: Vec::new(),
            },
        ]);
        response.model = ModelId::new("models/gemini-2.5-flash").unwrap();
        response.message = Message::assistant_text("Answer: fixture-secret").unwrap();
        response
    }

    fn qualified_file_search(response: &GenerateResponse) -> Result<(), String> {
        HostedEvidence::assess(
            response,
            Some(HostedAnswer::Marker("fixture-secret")),
            HostedEvidence::file_search(
                response,
                HostedProvider::Gemini,
                FileSearchContract::GeminiStreaming,
            ),
        )
    }

    fn assert_qualification_observation(response: &GenerateResponse) {
        let observation = FileSearchContract::GeminiStreaming.observation(response);
        assert_eq!(
            observation["contract"],
            "gemini_streaming_unpaired_file_search_declaration"
        );
        assert_eq!(
            observation["strict_file_search_only_requirement_met"],
            false
        );
        assert_eq!(
            observation["unpaired_file_search_declaration_observed"],
            true
        );
        assert_eq!(observation["code_execution_claimed"], false);
        assert_eq!(
            observation["response"],
            CaseFailure::response_evidence(response)
        );
        assert_eq!(
            observation["response"]["hosted_tool_outputs"],
            serde_json::to_value(&response.hosted_tool_outputs).unwrap(),
            "qualification must preserve the exact declaration bytes, shape and output order"
        );
    }

    fn failure_code(status: Option<HostedToolStatus>) -> HostedToolOutput {
        HostedToolOutput::CodeExecution {
            status,
            code: vec![HostedExecutableCode {
                language: Some("PYTHON".into()),
                code: "raise RuntimeError(\"EXPECTED_SENTINEL\")".into(),
            }],
            results: Vec::new(),
            artifacts: Vec::new(),
        }
    }

    fn openai_failure_record(logs: &str) -> HostedToolOutput {
        HostedToolOutput::CodeExecution {
            status: Some(HostedToolStatus::Completed),
            code: vec![HostedExecutableCode {
                language: Some("PYTHON".into()),
                code: HostedEvidence::failure_source(HostedProvider::OpenAi, "EXPECTED_SENTINEL")
                    .unwrap(),
            }],
            results: vec![HostedCodeExecutionResult {
                status: None,
                logs: Some(logs.into()),
                stdout: None,
                stderr: None,
                exit_code: None,
            }],
            artifacts: Vec::new(),
        }
    }

    fn retrieval_failure() -> HostedToolStatus {
        HostedToolStatus::Failed {
            error: RemoteFailure::new(
                ErrorKind::ProviderRejected,
                NativeErrorDetails {
                    code: Some("url_not_accessible".into()),
                    ..Default::default()
                },
            )
            .unwrap(),
        }
    }

    fn stdout(mut output: HostedToolOutput) -> HostedToolOutput {
        if let HostedToolOutput::CodeExecution { results, .. } = &mut output {
            for result in results {
                result.stdout = result.logs.take().or_else(|| result.stdout.take());
            }
        }
        output
    }

    fn provider_error(status: u16, code: Option<&str>, message: &str) -> KitError {
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
            code: code.map(str::to_owned),
            message: Some(message.into()),
            ..Default::default()
        })
    }

    fn openai_missing_store_details() -> NativeErrorDetails {
        NativeErrorDetails {
            message: Some(format!(
                "Vector store with id ['{}'] not found.",
                Self::MISSING_OPENAI_STORE
            )),
            error_type: Some("invalid_request_error".into()),
            event_type: Some("error".into()),
            sequence_number: Some(1),
            ..Default::default()
        }
    }

    fn openai_stream_error(details: NativeErrorDetails) -> KitError {
        let failure: RemoteFailure = serde_json::from_value(serde_json::json!({
            "kind": "invalid_request",
            "basis": {"source": "native_error_type"},
            "details": details,
        }))
        .unwrap();
        KitError::from_remote(failure, Operation::GenerateStream, ErrorPhase::Stream)
    }
}

#[test]
fn search_requires_a_qualified_action_and_source_beyond_the_variant() {
    let mut output = HostedFixture::web(Some(HostedToolStatus::Completed));
    if let HostedToolOutput::WebSearch { results, .. } = &mut output {
        results.clear();
    }
    assert!(
        HostedEvidence::web_search(
            &HostedFixture::response(vec![output]),
            HostedProvider::OpenAi
        )
        .is_err()
    );
    let mut output = HostedFixture::web(Some(HostedToolStatus::Completed));
    if let HostedToolOutput::WebSearch { action, .. } = &mut output {
        *action = None;
    }
    assert!(
        HostedEvidence::web_search(
            &HostedFixture::response(vec![output]),
            HostedProvider::Anthropic
        )
        .is_err()
    );
}

#[test]
fn search_accepts_real_history_but_not_failed_or_invented_status() {
    let outputs = vec![
        HostedFixture::web(Some(HostedToolStatus::InProgress)),
        HostedFixture::web(Some(HostedToolStatus::Completed)),
    ];
    assert!(
        HostedEvidence::web_search(&HostedFixture::response(outputs), HostedProvider::Anthropic)
            .is_ok()
    );
    assert!(
        HostedEvidence::web_search(
            &HostedFixture::response(vec![HostedFixture::web(Some(HostedFixture::failure()))]),
            HostedProvider::OpenAi
        )
        .is_err()
    );
    assert!(
        HostedEvidence::web_search(
            &HostedFixture::response(vec![HostedFixture::web(Some(HostedToolStatus::Completed))]),
            HostedProvider::Gemini
        )
        .is_err()
    );
    assert!(
        HostedEvidence::web_search(
            &HostedFixture::response(vec![HostedFixture::web(None)]),
            HostedProvider::Gemini
        )
        .is_ok()
    );
}

#[test]
fn file_search_preserves_identity_free_grounding_and_rejects_synthetic_success() {
    let response = HostedFixture::response(vec![HostedToolOutput::FileSearch {
        status: None,
        queries: Vec::new(),
        results: Some(Vec::new()),
        citations: Vec::new(),
    }]);
    assert!(
        HostedEvidence::file_search(
            &response,
            HostedProvider::Gemini,
            FileSearchContract::Exclusive
        )
        .is_ok()
    );
    assert!(
        HostedEvidence::file_search(
            &response,
            HostedProvider::OpenAi,
            FileSearchContract::Exclusive
        )
        .is_err()
    );
    let response = HostedFixture::response(vec![HostedToolOutput::FileSearch {
        status: Some(HostedToolStatus::Completed),
        queries: vec!["fixture marker".into()],
        results: None,
        citations: Vec::new(),
    }]);
    assert!(
        HostedEvidence::file_search(
            &response,
            HostedProvider::Gemini,
            FileSearchContract::Exclusive
        )
        .is_err()
    );
    assert!(
        HostedEvidence::file_search(
            &response,
            HostedProvider::OpenAi,
            FileSearchContract::Exclusive
        )
        .is_err()
    );
    assert!(
        HostedEvidence::file_search(
            &HostedFixture::response(Vec::new()),
            HostedProvider::Gemini,
            FileSearchContract::Exclusive
        )
        .is_err()
    );
}

#[test]
fn file_search_rejects_the_recorded_unrequested_code_declaration() {
    let grounding = HostedToolOutput::FileSearch {
        status: None,
        queries: Vec::new(),
        results: Some(Vec::new()),
        citations: Vec::new(),
    };
    let mut response = HostedFixture::response(vec![grounding]);
    response.message = Message::assistant_text("Answer: fixture-secret").unwrap();
    HostedEvidence::assess(
        &response,
        Some(HostedAnswer::Marker("fixture-secret")),
        HostedEvidence::file_search(
            &response,
            HostedProvider::Gemini,
            FileSearchContract::Exclusive,
        ),
    )
    .expect("the correct marker with identity-free grounding is valid on its own");
    response.hosted_tool_outputs.insert(
        0,
        HostedToolOutput::CodeExecution {
            status: None,
            code: vec![HostedExecutableCode {
                language: Some("PYTHON".into()),
                code: "print(file_search.query(\"fixture document\"))".into(),
            }],
            results: Vec::new(),
            artifacts: Vec::new(),
        },
    );
    let error = HostedEvidence::assess(
        &response,
        Some(HostedAnswer::Marker("fixture-secret")),
        HostedEvidence::file_search(
            &response,
            HostedProvider::Gemini,
            FileSearchContract::Exclusive,
        ),
    )
    .expect_err("correct retrieval must not conceal an undeclared code output");
    assert!(error.contains("unexpected hosted family"), "{error}");
}

#[test]
fn file_search_rejects_other_families_in_either_output_order() {
    for (provider, grounding) in [
        (HostedProvider::OpenAi, HostedFixture::file()),
        (
            HostedProvider::Gemini,
            HostedToolOutput::FileSearch {
                status: None,
                queries: Vec::new(),
                results: Some(Vec::new()),
                citations: Vec::new(),
            },
        ),
    ] {
        HostedEvidence::file_search(
            &HostedFixture::response(vec![grounding.clone()]),
            provider,
            FileSearchContract::Exclusive,
        )
        .expect("each provider retains its honest successful grounding shape");
        for unrelated in [
            HostedFixture::code(None),
            HostedFixture::web(Some(HostedToolStatus::Completed)),
            HostedToolOutput::UrlContext {
                requested_url: Some(HostedFixture::URL.into()),
                retrieved_url: Some(HostedFixture::URL.into()),
                status: Some(HostedToolStatus::Completed),
                citations: Vec::new(),
            },
        ] {
            for outputs in [
                vec![unrelated.clone(), grounding.clone()],
                vec![grounding.clone(), unrelated],
            ] {
                let error = HostedEvidence::file_search(
                    &HostedFixture::response(outputs),
                    provider,
                    FileSearchContract::Exclusive,
                )
                .expect_err("unrelated activity cannot borrow valid File Search evidence");
                assert!(error.contains("unexpected hosted family"), "{error}");
            }
        }
    }
}

#[test]
fn gemini_streaming_file_search_accepts_one_unpaired_literal_query_declaration() {
    for source in [
        "print(file_search.query(\"fixture document\"))",
        " \tprint(file_search.query( 'fixture document' ))\n",
        r#"print(file_search.query("fixture \"document\""))"#,
        r"print(file_search.query('fixture \'document\''))",
        r"print(file_search.query('fixture \\ document'))",
        r"print(file_search.query('fixture\ndocument\rrecord\tmarker'))",
        "print(file_search.query('fixture; document'))",
    ] {
        let response = HostedFixture::gemini_file_search_with_query(source);
        HostedFixture::qualified_file_search(&response).expect(
            "qualified retrieval retains the unpaired declaration without proving exclusion",
        );
        HostedFixture::assert_qualification_observation(&response);
    }
}

#[test]
fn gemini_streaming_file_search_accepts_one_unpaired_keyword_literal_declaration() {
    let mut response = HostedFixture::gemini_file_search_with_query(
        "print(file_search.query( query = 'fixture document' ))",
    );
    response.hosted_tool_outputs.reverse();
    HostedFixture::qualified_file_search(&response)
        .expect("literal query qualification does not depend on hosted output order");
    HostedFixture::assert_qualification_observation(&response);
}

#[test]
fn gemini_streaming_file_search_scope_requires_provider_operation_and_exact_selected_model() {
    for provider in [
        HostedProvider::OpenAi,
        HostedProvider::Anthropic,
        HostedProvider::Gemini,
    ] {
        for operation in [
            Operation::GenerateStream,
            Operation::Generate,
            Operation::SubmitBatch,
            Operation::SessionReceive,
            Operation::Unspecified,
        ] {
            let contract =
                FileSearchContract::for_request(provider, operation, "models/gemini-2.5-flash");
            let expected = if matches!(provider, HostedProvider::Gemini)
                && operation == Operation::GenerateStream
            {
                FileSearchContract::GeminiStreaming
            } else {
                FileSearchContract::Exclusive
            };
            assert_eq!(contract, expected);
        }
    }
    for selected in [
        "",
        "gemini-2.5-flash",
        "models/gemini-2.5-pro",
        "models/gemini-2.5-flash-lite",
        "models/gemini-2.5-flash-preview",
        "models/gemini-2.5-flash ",
        "Models/gemini-2.5-flash",
    ] {
        assert_eq!(
            FileSearchContract::for_request(
                HostedProvider::Gemini,
                Operation::GenerateStream,
                selected,
            ),
            FileSearchContract::Exclusive,
            "qualification must not normalize or substitute the selected model"
        );
    }
}

#[test]
fn gemini_streaming_file_search_qualification_does_not_apply_to_other_providers() {
    let mut response = HostedFixture::gemini_file_search_with_query(
        "print(file_search.query('fixture document'))",
    );
    response.hosted_tool_outputs[1] = HostedFixture::file();
    for provider in [HostedProvider::OpenAi, HostedProvider::Anthropic] {
        assert!(
            HostedEvidence::file_search(&response, provider, FileSearchContract::GeminiStreaming)
                .is_err(),
            "the contract variant cannot authorize another provider's query declaration"
        );
    }
}

#[test]
fn gemini_streaming_file_search_without_code_keeps_exclusion_proven() {
    let mut response = HostedFixture::gemini_file_search_with_query(
        "print(file_search.query('fixture document'))",
    );
    response.hosted_tool_outputs.remove(0);
    HostedFixture::qualified_file_search(&response)
        .expect("ordinary genuine retrieval remains accepted without a declaration");
    let observation = FileSearchContract::GeminiStreaming.observation(&response);
    assert_eq!(observation["strict_file_search_only_requirement_met"], true);
    assert_eq!(
        observation["unpaired_file_search_declaration_observed"],
        false
    );
    assert_eq!(observation["code_execution_claimed"], false);
    assert_eq!(
        observation["response"],
        CaseFailure::response_evidence(&response)
    );
}

#[test]
fn gemini_streaming_file_search_rejects_code_outside_the_single_literal_query() {
    for source in [
        "print(file_search.query(\"\"))",
        "print(file_search.query(\"   \"))",
        "print(file_search.query(query=''))",
        "print(file_search.query(query))",
        "print(file_search.query(query=query))",
        "print(file_search.query('fixture' + ' document'))",
        "print(file_search.query(str('fixture document')))",
        "print(file_search.query(f'fixture document'))",
        "print(file_search.query(r'fixture document'))",
        "print(file_search.query(b'fixture document'))",
        "print(file_search.query('''fixture document'''))",
        "print(file_search.query('fixture document')) # extra comment",
        "print(file_search.query('fixture\ndocument'))",
        r"print(file_search.query('fixture\x20document'))",
        r"print(file_search.query('fixture\u0020document'))",
        "print(file_search.query('fixture document', 'extra'))",
        "print(file_search.query(query='fixture document', top_k=2))",
        "print(file_search.query('fixture document')); print('extra')",
        "print(file_search.query('fixture document'))\nprint('extra')",
        "import os\nprint(file_search.query('fixture document'))",
        "print(other.query('fixture document'))",
        "file_search.query('fixture document')",
        "print(file_search.query('fixture document'))()",
        "print(file_search.query('fixture document')",
        "print(file_search.query('fixture document))",
    ] {
        let response = HostedFixture::gemini_file_search_with_query(source);
        assert!(
            HostedFixture::qualified_file_search(&response).is_err(),
            "the exception cannot authorize other syntax: {source:?}"
        );
    }
}

#[test]
fn gemini_streaming_file_search_rejects_other_or_missing_language() {
    for language in [None, Some("python"), Some("JAVASCRIPT"), Some("")] {
        let mut response = HostedFixture::gemini_file_search_with_query(
            "print(file_search.query('fixture document'))",
        );
        let HostedToolOutput::CodeExecution { code, .. } = &mut response.hosted_tool_outputs[0]
        else {
            panic!("query declaration fixture");
        };
        code[0].language = language.map(str::to_owned);
        assert!(HostedFixture::qualified_file_search(&response).is_err());
    }
}

#[test]
fn gemini_streaming_file_search_rejects_every_claimed_execution_status() {
    for status in [
        HostedToolStatus::InProgress,
        HostedToolStatus::Completed,
        HostedToolStatus::Incomplete,
        HostedFixture::failure(),
    ] {
        let mut response = HostedFixture::gemini_file_search_with_query(
            "print(file_search.query('fixture document'))",
        );
        let HostedToolOutput::CodeExecution { status: actual, .. } =
            &mut response.hosted_tool_outputs[0]
        else {
            panic!("query declaration fixture");
        };
        *actual = Some(status);
        assert!(HostedFixture::qualified_file_search(&response).is_err());
    }
}

#[test]
fn gemini_streaming_file_search_rejects_any_execution_result() {
    for status in [
        None,
        Some(HostedToolStatus::Completed),
        Some(HostedFixture::failure()),
    ] {
        let mut response = HostedFixture::gemini_file_search_with_query(
            "print(file_search.query('fixture document'))",
        );
        let HostedToolOutput::CodeExecution { results, .. } = &mut response.hosted_tool_outputs[0]
        else {
            panic!("query declaration fixture");
        };
        results.push(HostedCodeExecutionResult {
            status,
            logs: None,
            stdout: None,
            stderr: None,
            exit_code: None,
        });
        assert!(
            HostedFixture::qualified_file_search(&response).is_err(),
            "even an empty result is paired activity outside the exception"
        );
    }
}

#[test]
fn gemini_streaming_file_search_rejects_execution_or_response_artifacts() {
    let artifact = Artifact::Inline {
        media_type: "text/plain".into(),
        file_name: Some("unexpected.txt".into()),
        description: None,
        data: b"unexpected execution output".to_vec(),
    };
    let mut response = HostedFixture::gemini_file_search_with_query(
        "print(file_search.query('fixture document'))",
    );
    response.artifacts.push(artifact.clone());
    assert!(HostedFixture::qualified_file_search(&response).is_err());

    response.artifacts.clear();
    let HostedToolOutput::CodeExecution { artifacts, .. } = &mut response.hosted_tool_outputs[0]
    else {
        panic!("query declaration fixture");
    };
    artifacts.push(artifact);
    assert!(HostedFixture::qualified_file_search(&response).is_err());
}

#[test]
fn gemini_streaming_file_search_rejects_empty_or_multiple_declarations() {
    for count in [0, 2] {
        let mut response = HostedFixture::gemini_file_search_with_query(
            "print(file_search.query('fixture document'))",
        );
        let HostedToolOutput::CodeExecution { code, .. } = &mut response.hosted_tool_outputs[0]
        else {
            panic!("query declaration fixture");
        };
        code.resize(count, code[0].clone());
        assert!(HostedFixture::qualified_file_search(&response).is_err());
    }
    let mut response = HostedFixture::gemini_file_search_with_query(
        "print(file_search.query('fixture document'))",
    );
    response
        .hosted_tool_outputs
        .push(response.hosted_tool_outputs[0].clone());
    assert!(HostedFixture::qualified_file_search(&response).is_err());
}

#[test]
fn gemini_streaming_file_search_rejects_any_additional_hosted_family() {
    for extra in [
        HostedFixture::web(None),
        HostedToolOutput::UrlContext {
            requested_url: None,
            retrieved_url: Some(HostedFixture::URL.into()),
            status: None,
            citations: Vec::new(),
        },
    ] {
        let mut response = HostedFixture::gemini_file_search_with_query(
            "print(file_search.query('fixture document'))",
        );
        response.hosted_tool_outputs.push(extra);
        assert!(HostedFixture::qualified_file_search(&response).is_err());
    }
}

#[test]
fn gemini_streaming_file_search_still_requires_genuine_grounding() {
    let mut response = HostedFixture::gemini_file_search_with_query(
        "print(file_search.query('fixture document'))",
    );
    response.hosted_tool_outputs.remove(1);
    assert!(HostedFixture::qualified_file_search(&response).is_err());

    let mut response = HostedFixture::gemini_file_search_with_query(
        "print(file_search.query('fixture document'))",
    );
    let HostedToolOutput::FileSearch { results, .. } = &mut response.hosted_tool_outputs[1] else {
        panic!("grounding fixture");
    };
    *results = None;
    assert!(HostedFixture::qualified_file_search(&response).is_err());

    for status in [
        HostedToolStatus::Completed,
        HostedToolStatus::Incomplete,
        HostedFixture::failure(),
    ] {
        let mut response = HostedFixture::gemini_file_search_with_query(
            "print(file_search.query('fixture document'))",
        );
        let HostedToolOutput::FileSearch { status: actual, .. } =
            &mut response.hosted_tool_outputs[1]
        else {
            panic!("grounding fixture");
        };
        *actual = Some(status);
        assert!(HostedFixture::qualified_file_search(&response).is_err());
    }
}

#[test]
fn gemini_streaming_file_search_still_requires_the_marker_and_successful_finish() {
    let mut response = HostedFixture::gemini_file_search_with_query(
        "print(file_search.query('fixture document'))",
    );
    response.message = Message::assistant_text("Answer: wrong-marker").unwrap();
    assert!(HostedFixture::qualified_file_search(&response).is_err());
    response.message = Message::assistant_text("Answer: fixture-secret").unwrap();
    for reason in [
        None,
        Some(FinishReason::Length),
        Some(FinishReason::ToolCall),
    ] {
        response.finish_reason = reason;
        assert!(HostedFixture::qualified_file_search(&response).is_err());
    }
}

#[test]
fn gemini_streaming_file_search_rejects_an_unexpected_function_call() {
    let mut response = HostedFixture::gemini_file_search_with_query(
        "print(file_search.query('fixture document'))",
    );
    response.message = Message::assistant(
        vec![ContentPart::Text {
            text: "Answer: fixture-secret".into(),
        }],
        vec![FunctionCall::new(None, "unexpected", serde_json::json!({})).unwrap()],
    )
    .unwrap();
    assert!(HostedFixture::qualified_file_search(&response).is_err());
}

#[test]
fn openai_file_search_requires_included_results_and_a_matching_file_citation() {
    let valid = HostedFixture::response(vec![HostedFixture::file()]);
    assert!(
        HostedEvidence::file_search(
            &valid,
            HostedProvider::OpenAi,
            FileSearchContract::Exclusive
        )
        .is_ok()
    );
    for results in [None, Some(Vec::new())] {
        let mut response = valid.clone();
        if let HostedToolOutput::FileSearch {
            results: actual, ..
        } = &mut response.hosted_tool_outputs[0]
        {
            *actual = results;
        }
        assert!(
            HostedEvidence::file_search(
                &response,
                HostedProvider::OpenAi,
                FileSearchContract::Exclusive
            )
            .is_err()
        );
    }
    let mut response = valid.clone();
    if let HostedToolOutput::FileSearch { citations, .. } = &mut response.hosted_tool_outputs[0] {
        *citations = vec![Citation::Url {
            url: HostedFixture::URL.into(),
            title: None,
            snippet: None,
            start_index: None,
            end_index: None,
        }];
    }
    assert!(
        HostedEvidence::file_search(
            &response,
            HostedProvider::OpenAi,
            FileSearchContract::Exclusive
        )
        .is_err()
    );
    let mut response = valid;
    if let HostedToolOutput::FileSearch { citations, .. } = &mut response.hosted_tool_outputs[0]
        && let Citation::File {
            reference: FileReference::Provider { id, .. },
            ..
        } = &mut citations[0]
    {
        *id = "file-unrelated".into();
    }
    assert!(
        HostedEvidence::file_search(
            &response,
            HostedProvider::OpenAi,
            FileSearchContract::Exclusive
        )
        .is_err()
    );
}

#[test]
fn url_success_requires_exact_source_and_native_gemini_retrieval_success() {
    let response = HostedFixture::response(vec![HostedToolOutput::UrlContext {
        requested_url: None,
        retrieved_url: Some(HostedFixture::URL.into()),
        status: Some(HostedFixture::failure()),
        citations: vec![Citation::Url {
            url: HostedFixture::URL.into(),
            title: None,
            snippet: Some("URL_RETRIEVAL_STATUS_ERROR".into()),
            start_index: None,
            end_index: None,
        }],
    }]);
    assert!(
        HostedEvidence::url_context(&response, HostedProvider::Gemini, HostedFixture::URL).is_err()
    );
    assert!(
        HostedEvidence::url_context(
            &response,
            HostedProvider::Anthropic,
            "https://other.invalid/"
        )
        .is_err()
    );
    let mut valid = response;
    if let HostedToolOutput::UrlContext {
        status, citations, ..
    } = &mut valid.hosted_tool_outputs[0]
    {
        *status = Some(HostedToolStatus::Completed);
        citations.clear();
    }
    assert!(
        HostedEvidence::url_context(&valid, HostedProvider::Gemini, HostedFixture::URL).is_ok()
    );
}

#[test]
fn url_failure_rejects_completed_and_absent_failure_records() {
    assert!(
        HostedEvidence::url_failure(
            &HostedFixture::response(Vec::new()),
            HostedProvider::Anthropic,
            HostedFixture::URL
        )
        .is_err()
    );
    assert!(
        HostedEvidence::url_failure(
            &HostedFixture::response(vec![HostedToolOutput::UrlContext {
                requested_url: Some(HostedFixture::URL.into()),
                retrieved_url: None,
                status: Some(HostedToolStatus::Completed),
                citations: Vec::new(),
            }]),
            HostedProvider::Anthropic,
            HostedFixture::URL
        )
        .is_err()
    );
    assert!(
        HostedEvidence::url_failure(
            &HostedFixture::response(vec![
                HostedToolOutput::UrlContext {
                    requested_url: Some(HostedFixture::URL.into()),
                    retrieved_url: None,
                    status: Some(HostedToolStatus::InProgress),
                    citations: Vec::new()
                },
                HostedToolOutput::UrlContext {
                    requested_url: Some(HostedFixture::URL.into()),
                    retrieved_url: None,
                    status: Some(HostedFixture::retrieval_failure()),
                    citations: Vec::new()
                },
            ]),
            HostedProvider::Anthropic,
            HostedFixture::URL
        )
        .is_ok()
    );
}

#[test]
fn code_success_requires_execution_result_and_exact_stdout_line() {
    assert!(
        HostedEvidence::code_success(
            &HostedFixture::response(vec![HostedFixture::code(None)]),
            HostedProvider::Gemini,
            "338350",
        )
        .is_err()
    );
    let valid = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::Gemini,
        HostedFixture::code(None),
        HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedToolStatus::Completed),
            "338350\n",
            None,
        ),
    )]);
    assert!(HostedEvidence::code_success(&valid, HostedProvider::Gemini, "338350").is_ok());
    assert!(HostedEvidence::code_success(&valid, HostedProvider::Gemini, "33835").is_err());
    let synthetic = HostedFixture::response(vec![
        HostedFixture::code(Some(HostedToolStatus::InProgress)),
        HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedToolStatus::Completed),
            "338350\n",
            None,
        ),
    ]);
    assert!(HostedEvidence::code_success(&synthetic, HostedProvider::Gemini, "338350").is_err());
    assert!(HostedEvidence::code_success(&synthetic, HostedProvider::Anthropic, "338350").is_err());
    for failure in [
        HostedFixture::result(
            Some(HostedFixture::failure()),
            Some(HostedToolStatus::Completed),
            "338350\n",
            None,
        ),
        HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedFixture::failure()),
            "338350\n",
            None,
        ),
        HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedToolStatus::Completed),
            "338350\n",
            Some(1),
        ),
    ] {
        let mut response = valid.clone();
        response.hosted_tool_outputs.push(failure);
        assert!(HostedEvidence::code_success(&response, HostedProvider::Gemini, "338350").is_err());
    }
}

#[test]
fn code_failure_rejects_model_text_and_nonmatching_execution_logs() {
    let response = HostedFixture::response(vec![HostedFixture::code(None)]);
    assert!(
        HostedEvidence::code_failure(&response, HostedProvider::Gemini, "EXPECTED_SENTINEL")
            .is_err()
    );
    let response = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::Gemini,
        HostedFixture::failure_code(None),
        HostedFixture::result(
            Some(HostedFixture::failure()),
            Some(HostedFixture::failure()),
            "Traceback (most recent call last):\nRuntimeError: EXPECTED_SENTINEL_extra\n",
            None,
        ),
    )]);
    assert!(
        HostedEvidence::code_failure(&response, HostedProvider::Gemini, "EXPECTED_SENTINEL")
            .is_err()
    );
    let response = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::Gemini,
        HostedFixture::failure_code(None),
        HostedFixture::result(
            Some(HostedFixture::failure()),
            Some(HostedFixture::failure()),
            "The tool said RuntimeError: EXPECTED_SENTINEL",
            None,
        ),
    )]);
    assert!(
        HostedEvidence::code_failure(&response, HostedProvider::Gemini, "EXPECTED_SENTINEL")
            .is_err()
    );
    let mut fake_code = HostedFixture::failure_code(None);
    if let HostedToolOutput::CodeExecution { code, .. } = &mut fake_code {
        code[0].code =
            "print('Traceback (most recent call last):\\nRuntimeError: EXPECTED_SENTINEL')".into();
    }
    let response = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::OpenAi,
        fake_code,
        HostedFixture::result(
            Some(HostedToolStatus::Completed),
            None,
            "Traceback (most recent call last):\nRuntimeError: EXPECTED_SENTINEL\n",
            None,
        ),
    )]);
    assert!(
        HostedEvidence::code_failure(&response, HostedProvider::OpenAi, "EXPECTED_SENTINEL")
            .is_err()
    );
}

#[test]
fn code_failure_accepts_each_native_failure_representation() {
    let logs =
        "Traceback (most recent call last):\n  file probe.py\nRuntimeError: EXPECTED_SENTINEL\n";
    let gemini = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::Gemini,
        HostedFixture::failure_code(None),
        HostedFixture::result(
            Some(HostedFixture::failure()),
            Some(HostedFixture::failure()),
            logs,
            None,
        ),
    )]);
    let mut command = HostedFixture::failure_code(Some(HostedToolStatus::InProgress));
    if let HostedToolOutput::CodeExecution { code, .. } = &mut command {
        code[0].code =
            HostedEvidence::failure_source(HostedProvider::Anthropic, "EXPECTED_SENTINEL").unwrap();
    }
    let anthropic = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::Anthropic,
        command,
        HostedFixture::stderr(HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedFixture::failure()),
            logs,
            Some(1),
        )),
    )]);
    let openai = HostedFixture::response(vec![HostedFixture::openai_failure_record(logs)]);
    assert!(
        HostedEvidence::code_failure(&gemini, HostedProvider::Gemini, "EXPECTED_SENTINEL").is_ok()
    );
    assert!(
        HostedEvidence::code_failure(&anthropic, HostedProvider::Anthropic, "EXPECTED_SENTINEL")
            .is_ok()
    );
    assert!(
        HostedEvidence::code_failure(&openai, HostedProvider::OpenAi, "EXPECTED_SENTINEL").is_ok()
    );
}

#[test]
fn code_failure_accepts_native_exception_without_a_conventional_traceback_header() {
    for logs in [
        "RuntimeError: EXPECTED_SENTINEL\n",
        "\u{1b}[31mRuntimeError\u{1b}[0m: EXPECTED_SENTINEL\n",
        "Cell In[1], line 1\n----> 1 raise RuntimeError(\"EXPECTED_SENTINEL\")\nRuntimeError: EXPECTED_SENTINEL\n",
    ] {
        let response = HostedFixture::response(vec![HostedFixture::openai_failure_record(logs)]);
        assert!(
            HostedEvidence::code_failure(&response, HostedProvider::OpenAi, "EXPECTED_SENTINEL")
                .is_ok(),
            "a paired exact raising source and native exception line must not require one traceback renderer"
        );
    }
}

#[test]
fn code_failure_accepts_the_anthropic_native_shell_command() {
    let mut declaration = HostedFixture::failure_code(Some(HostedToolStatus::InProgress));
    if let HostedToolOutput::CodeExecution { code, .. } = &mut declaration {
        code[0].language = Some("bash".into());
        code[0].code = "python3 -c 'raise RuntimeError(\"EXPECTED_SENTINEL\")'".into();
    }
    let response = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::Anthropic,
        declaration,
        HostedFixture::stderr(HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedFixture::failure()),
            "RuntimeError: EXPECTED_SENTINEL\n",
            Some(1),
        )),
    )]);
    assert!(
        HostedEvidence::code_failure(&response, HostedProvider::Anthropic, "EXPECTED_SENTINEL")
            .is_ok(),
        "the hosted Bash runtime must be tested with a real command rather than bare Python equality"
    );
}

#[test]
fn code_success_cannot_pair_a_source_with_an_unrelated_result_record() {
    let response = HostedFixture::response(vec![
        HostedFixture::code(Some(HostedToolStatus::Completed)),
        HostedFixture::result(Some(HostedToolStatus::Completed), None, "338350\n", None),
    ]);
    assert!(
        HostedEvidence::code_success(&response, HostedProvider::OpenAi, "338350").is_err(),
        "OpenAI source and output must belong to the same interpreter call"
    );

    let response = HostedFixture::response(vec![
        HostedFixture::code(None),
        HostedFixture::code(None),
        HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedToolStatus::Completed),
            "338350\n",
            None,
        ),
    ]);
    assert!(
        HostedEvidence::code_success(&response, HostedProvider::Gemini, "338350").is_err(),
        "two pending declarations make the public source/result association ambiguous"
    );
}

#[test]
fn code_proof_never_repairs_split_native_source_and_result_records() {
    for provider in [HostedProvider::Gemini, HostedProvider::Anthropic] {
        let mut source = HostedFixture::failure_code(None);
        if let HostedToolOutput::CodeExecution { code, .. } = &mut source {
            code[0].code = HostedEvidence::failure_source(provider, "EXPECTED_SENTINEL").unwrap();
        }
        let mut failed = HostedFixture::result(
            Some(HostedFixture::failure()),
            Some(HostedFixture::failure()),
            "RuntimeError: EXPECTED_SENTINEL\n",
            matches!(provider, HostedProvider::Anthropic).then_some(1),
        );
        if matches!(provider, HostedProvider::Anthropic) {
            failed = HostedFixture::stderr(failed);
        }
        assert!(
            HostedEvidence::code_failure(
                &HostedFixture::response(vec![source, failed]),
                provider,
                "EXPECTED_SENTINEL"
            )
            .is_err(),
            "split records may retain unrelated native identities or a prior-turn result"
        );

        let success = HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedToolStatus::Completed),
            "338350\n",
            matches!(provider, HostedProvider::Anthropic).then_some(0),
        );
        assert!(
            HostedEvidence::code_success(
                &HostedFixture::response(vec![HostedFixture::code(None), success]),
                provider,
                "338350"
            )
            .is_err(),
            "the test must not invent an association discarded or unresolved by the native normalizer"
        );
    }
}

#[test]
fn file_search_reports_missing_results_and_citations_separately() {
    let mut output = HostedFixture::file();
    if let HostedToolOutput::FileSearch {
        results, citations, ..
    } = &mut output
    {
        *results = None;
        citations.clear();
    }
    let error = HostedEvidence::file_search(
        &HostedFixture::response(vec![output]),
        HostedProvider::OpenAi,
        FileSearchContract::Exclusive,
    )
    .unwrap_err();
    assert!(error.contains("results=omitted"), "{error}");
    assert!(error.contains("file_citations=0"), "{error}");
}

#[test]
fn consulted_web_sources_do_not_substitute_for_an_answer_citation() {
    let mut output = HostedFixture::web(Some(HostedToolStatus::Completed));
    if let HostedToolOutput::WebSearch { citations, .. } = &mut output {
        citations.clear();
    }
    let response = HostedFixture::response(vec![output]);
    assert!(
        HostedEvidence::web_search(&response, HostedProvider::OpenAi).is_err(),
        "a consulted source without a source citation does not prove answer attribution"
    );
}

#[test]
fn code_failure_cannot_borrow_a_source_from_an_unrelated_execution() {
    let logs = "Traceback (most recent call last):\nRuntimeError: EXPECTED_SENTINEL\n";
    let mut printed = HostedFixture::openai_failure_record(logs);
    if let HostedToolOutput::CodeExecution { code, .. } = &mut printed {
        code[0].code =
            "print('Traceback (most recent call last):\\nRuntimeError: EXPECTED_SENTINEL')".into();
    }
    let response = HostedFixture::response(vec![
        HostedFixture::failure_code(Some(HostedToolStatus::Completed)),
        printed,
    ]);
    assert!(
        HostedEvidence::code_failure(&response, HostedProvider::OpenAi, "EXPECTED_SENTINEL")
            .is_err()
    );
    let response = HostedFixture::response(vec![
        HostedFixture::failure_code(Some(HostedToolStatus::Completed)),
        HostedFixture::result(Some(HostedToolStatus::Completed), None, logs, None),
    ]);
    assert!(
        HostedEvidence::code_failure(&response, HostedProvider::OpenAi, "EXPECTED_SENTINEL")
            .is_err()
    );

    for (provider, code_status, exit_code) in [
        (HostedProvider::Gemini, None, None),
        (
            HostedProvider::Anthropic,
            Some(HostedToolStatus::InProgress),
            Some(1),
        ),
    ] {
        let mut unrelated = HostedFixture::failure_code(code_status.clone());
        if let HostedToolOutput::CodeExecution { code, .. } = &mut unrelated {
            code[0].code =
                "print('Traceback (most recent call last):\\nRuntimeError: EXPECTED_SENTINEL')"
                    .into();
        }
        let response = HostedFixture::response(vec![
            HostedFixture::failure_code(code_status),
            unrelated,
            HostedFixture::result(
                Some(HostedFixture::failure()),
                Some(HostedFixture::failure()),
                logs,
                exit_code,
            ),
        ]);
        assert!(HostedEvidence::code_failure(&response, provider, "EXPECTED_SENTINEL").is_err());
    }
}

#[test]
fn code_failure_keeps_a_proven_failure_when_later_execution_recovers() {
    let response = HostedFixture::response(vec![
        HostedFixture::paired(
            HostedProvider::Gemini,
            HostedFixture::failure_code(None),
            HostedFixture::result(
                Some(HostedFixture::failure()),
                Some(HostedFixture::failure()),
                "Traceback (most recent call last):\nRuntimeError: EXPECTED_SENTINEL\n",
                None,
            ),
        ),
        HostedFixture::paired(
            HostedProvider::Gemini,
            HostedFixture::code(None),
            HostedFixture::result(
                Some(HostedToolStatus::Completed),
                Some(HostedToolStatus::Completed),
                "338350\n",
                None,
            ),
        ),
    ]);
    assert!(
        HostedEvidence::code_failure(&response, HostedProvider::Gemini, "EXPECTED_SENTINEL")
            .is_ok()
    );
}

#[test]
fn hosted_records_cannot_hide_unsuccessful_generation() {
    let mut response =
        HostedFixture::response(vec![HostedFixture::web(Some(HostedToolStatus::Completed))]);
    assert!(
        HostedEvidence::assess(
            &response,
            None,
            HostedEvidence::web_search(&response, HostedProvider::OpenAi)
        )
        .is_ok()
    );
    response.finish_reason = Some(FinishReason::Error);
    assert!(
        HostedEvidence::assess(
            &response,
            None,
            HostedEvidence::web_search(&response, HostedProvider::OpenAi)
        )
        .is_err()
    );
    let mut response = HostedFixture::response(vec![HostedToolOutput::UrlContext {
        requested_url: Some(HostedFixture::URL.into()),
        retrieved_url: None,
        status: Some(HostedFixture::retrieval_failure()),
        citations: Vec::new(),
    }]);
    assert!(
        HostedEvidence::assess(
            &response,
            None,
            HostedEvidence::url_failure(&response, HostedProvider::Anthropic, HostedFixture::URL)
        )
        .is_ok()
    );
    response.finish_reason = Some(FinishReason::Length);
    assert!(
        HostedEvidence::assess(
            &response,
            None,
            HostedEvidence::url_failure(&response, HostedProvider::Anthropic, HostedFixture::URL)
        )
        .is_err()
    );
}

#[test]
fn missing_store_requires_exact_resource_and_cannot_pass_auth_or_quota_errors() {
    let missing = "vs_strict-fixture";
    let message = "Vector store with id 'vs_strict-fixture' not found.";
    assert!(
        HostedEvidence::missing_store(
            &HostedFixture::provider_error(404, None, message),
            Operation::Generate,
            HostedProvider::OpenAi,
            missing,
        )
        .is_ok()
    );
    let gemini_missing = "fileSearchStores/strict-fixture";
    assert!(
        HostedEvidence::missing_store(
            &HostedFixture::provider_error(
                404,
                Some("NOT_FOUND"),
                "Resource 'fileSearchStores/strict-fixture' not found."
            ),
            Operation::Generate,
            HostedProvider::Gemini,
            gemini_missing,
        )
        .is_ok()
    );
    for error in [
        HostedFixture::provider_error(404, Some("insufficient_quota"), message),
        HostedFixture::provider_error(404, Some("invalid_api_key"), message),
        HostedFixture::provider_error(401, None, message),
        HostedFixture::provider_error(429, None, message),
        HostedFixture::provider_error(400, None, message),
        HostedFixture::provider_error(404, None, "The selected model was not found."),
        HostedFixture::provider_error(
            404,
            None,
            "Vector store vs_strict-fixture-extra was not found.",
        ),
    ] {
        assert!(
            HostedEvidence::missing_store(
                &error,
                Operation::Generate,
                HostedProvider::OpenAi,
                missing
            )
            .is_err()
        );
    }
    for error in [
        HostedFixture::provider_error(
            404,
            Some("RESOURCE_EXHAUSTED"),
            "fileSearchStores/strict-fixture",
        ),
        HostedFixture::provider_error(
            404,
            Some("PERMISSION_DENIED"),
            "fileSearchStores/strict-fixture",
        ),
        HostedFixture::provider_error(
            404,
            Some("NOT_FOUND"),
            "Another file-search store was not found.",
        ),
    ] {
        assert!(
            HostedEvidence::missing_store(
                &error,
                Operation::Generate,
                HostedProvider::Gemini,
                gemini_missing
            )
            .is_err()
        );
    }
}

#[test]
fn missing_store_accepts_http_rejection_and_classified_in_stream_error() {
    for (provider, resource, code) in [
        (HostedProvider::OpenAi, "vs_strict-fixture", "not_found"),
        (
            HostedProvider::OpenAi,
            "vs_strict-fixture",
            "resource_not_found",
        ),
        (
            HostedProvider::Gemini,
            "fileSearchStores/strict-fixture",
            "NOT_FOUND",
        ),
        (
            HostedProvider::Gemini,
            "fileSearchStores/strict-fixture",
            "404",
        ),
    ] {
        let details = NativeErrorDetails {
            code: Some(code.to_owned()),
            message: Some(format!("Resource '{resource}' not found")),
            ..Default::default()
        };
        for operation in [Operation::Generate, Operation::GenerateStream] {
            let error = KitError::new(ErrorKind::NotFound, operation, ErrorPhase::ResponseHeaders)
                .with_http_status(404)
                .with_native_details(details.clone());
            assert!(HostedEvidence::missing_store(&error, operation, provider, resource).is_ok());
        }
        let error = KitError::new(
            ErrorKind::NotFound,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(details);
        assert!(
            HostedEvidence::missing_store(&error, Operation::GenerateStream, provider, resource)
                .is_ok(),
            "a recognized scenario code and stream context satisfy the assertion contract without an HTTP error status"
        );
    }
}

#[test]
fn missing_store_rejects_inconsistent_failure_context() {
    for (kind, operation, phase, status) in [
        (
            ErrorKind::Authentication,
            Operation::Generate,
            ErrorPhase::ResponseHeaders,
            Some(404),
        ),
        (
            ErrorKind::NotFound,
            Operation::FileInfo,
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
        let mut error =
            KitError::new(kind, operation, phase).with_native_details(NativeErrorDetails {
                code: Some("resource_not_found".into()),
                message: Some("Resource 'vs_strict-fixture' not found".into()),
                ..Default::default()
            });
        if let Some(status) = status {
            error = error.with_http_status(status);
        }
        assert!(
            HostedEvidence::missing_store(
                &error,
                Operation::Generate,
                HostedProvider::OpenAi,
                "vs_strict-fixture"
            )
            .is_err(),
            "inconsistent failure context must not prove the scenario: {error:?}"
        );
    }
}

#[test]
fn missing_store_stream_requires_explicit_code_and_exact_resource() {
    for (code, message) in [
        (None, "vs_strict-fixture"),
        (Some(""), "vs_strict-fixture"),
        (Some(" "), "vs_strict-fixture"),
        (Some("insufficient_quota"), "vs_strict-fixture"),
        (Some("resource_not_found_extra"), "vs_strict-fixture"),
        (Some("resource_not_found"), "different-store"),
        (Some("resource_not_found"), "vs_strict-fixture-extra"),
        (Some("resource_not_found"), "prefix-vs_strict-fixture"),
        (Some("resource_not_found"), ""),
    ] {
        let error = KitError::new(
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
            HostedEvidence::missing_store(
                &error,
                Operation::GenerateStream,
                HostedProvider::OpenAi,
                "vs_strict-fixture"
            )
            .is_err()
        );
    }
}

#[test]
fn openai_streamed_missing_store_accepts_the_observed_native_invalid_request() {
    let error = HostedFixture::openai_stream_error(HostedFixture::openai_missing_store_details());
    assert_eq!(error.kind(), ErrorKind::InvalidRequest);
    assert_eq!(
        error.classification_basis(),
        ClassificationBasis::NativeErrorType
    );
    assert!(error.remote_failure().is_some());
    assert!(error.http_status().is_none());
    assert!(error.native_details().details.is_empty());
    HostedEvidence::missing_store(
        &error,
        Operation::GenerateStream,
        HostedProvider::OpenAi,
        HostedFixture::MISSING_OPENAI_STORE,
    )
    .expect("the exact native streamed rejection proves the requested store is absent");
}

#[test]
fn openai_streamed_missing_store_rejects_generic_or_misattributed_diagnostics() {
    let missing = HostedFixture::MISSING_OPENAI_STORE;
    for message in [
        None,
        Some(String::new()),
        Some(missing.to_owned()),
        Some(format!("Invalid parameter vector_store_ids: {missing}")),
        Some(format!("Malformed vector store ID: {missing}")),
        Some(format!(
            "Vector store with id ['{missing}-extra'] not found."
        )),
        Some(format!(
            "Vector store with id ['prefix-{missing}'] not found."
        )),
        Some(format!(
            "Vector store with id ['https://example.com/{missing}'] not found."
        )),
        Some("Vector store with id ['vs_another_store'] not found.".into()),
        Some(format!(
            "Quoted diagnostic: Vector store with id ['{missing}'] not found."
        )),
        Some(format!(
            "Vector store with id ['{missing}'] not found. Check another parameter."
        )),
    ] {
        let mut details = HostedFixture::openai_missing_store_details();
        details.message = message;
        let error = HostedFixture::openai_stream_error(details)
            .with_request_target(missing)
            .with_correlation_id(missing);
        assert!(
            HostedEvidence::missing_store(
                &error,
                Operation::GenerateStream,
                HostedProvider::OpenAi,
                missing,
            )
            .is_err(),
            "caller identity and incidental native mentions cannot prove absence"
        );
    }
    for missing in ["", "vs_strict-fixture", "vs_strict-ABCDEF", "vs_other"] {
        let mut details = HostedFixture::openai_missing_store_details();
        details.message = Some(format!("Vector store with id ['{missing}'] not found."));
        let error = HostedFixture::openai_stream_error(details);
        assert!(
            HostedEvidence::missing_store(
                &error,
                Operation::GenerateStream,
                HostedProvider::OpenAi,
                missing,
            )
            .is_err(),
            "the observed-envelope exception is restricted to valid owned fixture IDs"
        );
    }
}

#[test]
fn openai_streamed_missing_store_rejects_contradictory_or_incomplete_native_details() {
    for (field, value) in [
        ("code", serde_json::json!("insufficient_quota")),
        ("code", serde_json::json!("invalid_api_key")),
        ("code", serde_json::json!("model_not_found")),
        ("code", serde_json::json!("resource_not_found")),
        ("error_type", serde_json::json!("authentication_error")),
        ("error_type", serde_json::json!("rate_limit_error")),
        ("error_type", serde_json::json!("error")),
        ("event_type", serde_json::Value::Null),
        ("event_type", serde_json::json!("response.failed")),
        ("parameter", serde_json::json!("model")),
        ("native_status", serde_json::json!("PERMISSION_DENIED")),
        ("native_status", serde_json::json!("NOT_FOUND")),
        ("reported_http_status", serde_json::json!(403)),
        ("reported_http_status", serde_json::json!(404)),
        ("rpc_code", serde_json::json!(7)),
        ("rpc_code", serde_json::json!(5)),
        ("outcome", serde_json::json!("OUTCOME_FAILED")),
        ("execution_exit_code", serde_json::json!(1)),
        ("truncated", serde_json::json!(true)),
    ] {
        let mut details =
            serde_json::to_value(HostedFixture::openai_missing_store_details()).unwrap();
        details[field] = value;
        let error = HostedFixture::openai_stream_error(serde_json::from_value(details).unwrap());
        assert!(
            HostedEvidence::missing_store(
                &error,
                Operation::GenerateStream,
                HostedProvider::OpenAi,
                HostedFixture::MISSING_OPENAI_STORE,
            )
            .is_err(),
            "contradictory native field {field} must remain rejected"
        );
    }
    let mut details = HostedFixture::openai_missing_store_details();
    details.truncated = true;
    details.omitted_details = 1;
    for records in [
        Vec::new(),
        vec![NativeDetail::Resource {
            resource_type: None,
            name: HostedFixture::MISSING_OPENAI_STORE.into(),
            owner: None,
            description: None,
        }],
        vec![NativeDetail::Resource {
            resource_type: None,
            name: "vs_another_store".into(),
            owner: None,
            description: None,
        }],
        vec![NativeDetail::QuotaViolation {
            subject: Some(HostedFixture::MISSING_OPENAI_STORE.into()),
            description: None,
        }],
        vec![NativeDetail::FieldViolation {
            field: "vector_store_ids".into(),
            reason: Some("malformed".into()),
            description: None,
        }],
    ] {
        let mut observed = if records.is_empty() {
            details.clone()
        } else {
            HostedFixture::openai_missing_store_details()
        };
        observed.details = records;
        let error = HostedFixture::openai_stream_error(observed);
        assert!(
            HostedEvidence::missing_store(
                &error,
                Operation::GenerateStream,
                HostedProvider::OpenAi,
                HostedFixture::MISSING_OPENAI_STORE,
            )
            .is_err()
        );
    }
}

#[test]
fn openai_streamed_missing_store_rejects_same_id_with_conflicting_resource_type() {
    let mut details = HostedFixture::openai_missing_store_details();
    details.details.push(NativeDetail::Resource {
        resource_type: Some("model".into()),
        name: HostedFixture::MISSING_OPENAI_STORE.into(),
        owner: None,
        description: None,
    });
    let error = HostedFixture::openai_stream_error(details);
    assert!(
        HostedEvidence::missing_store(
            &error,
            Operation::GenerateStream,
            HostedProvider::OpenAi,
            HostedFixture::MISSING_OPENAI_STORE,
        )
        .is_err(),
        "a matching name with a conflicting native resource type cannot prove store absence"
    );
}

#[test]
fn openai_streamed_missing_store_rejects_wrong_kind_basis_and_context() {
    let observed =
        HostedFixture::openai_stream_error(HostedFixture::openai_missing_store_details());
    for kind in [
        "authentication",
        "permission_denied",
        "rate_limited",
        "quota_exceeded",
        "billing",
        "resource_exhausted",
        "provider_rejected",
    ] {
        let mut failure = serde_json::to_value(observed.remote_failure().unwrap()).unwrap();
        failure["kind"] = serde_json::json!(kind);
        let error = KitError::from_remote(
            serde_json::from_value(failure).unwrap(),
            Operation::GenerateStream,
            ErrorPhase::Stream,
        );
        assert!(
            HostedEvidence::missing_store(
                &error,
                Operation::GenerateStream,
                HostedProvider::OpenAi,
                HostedFixture::MISSING_OPENAI_STORE,
            )
            .is_err()
        );
    }
    let remote = observed.remote_failure().unwrap().clone();
    for (operation, phase) in [
        (Operation::Generate, ErrorPhase::Stream),
        (Operation::FileInfo, ErrorPhase::Stream),
        (Operation::GenerateStream, ErrorPhase::ResponseHeaders),
        (Operation::GenerateStream, ErrorPhase::Normalize),
    ] {
        let error = KitError::from_remote(remote.clone(), operation, phase);
        assert!(
            HostedEvidence::missing_store(
                &error,
                operation,
                HostedProvider::OpenAi,
                HostedFixture::MISSING_OPENAI_STORE,
            )
            .is_err()
        );
    }
    for error in [
        observed.clone().with_row(0),
        observed.clone().with_http_status(200),
        observed.clone().with_http_status(401),
        observed.clone().with_http_status(404),
        observed.clone().with_http_status(429),
        KitError::from_remote(
            RemoteFailure::new(
                ErrorKind::InvalidRequest,
                HostedFixture::openai_missing_store_details(),
            )
            .unwrap(),
            Operation::GenerateStream,
            ErrorPhase::Stream,
        ),
        KitError::new(
            ErrorKind::InvalidRequest,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(HostedFixture::openai_missing_store_details()),
        KitError::new(
            ErrorKind::Transport,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(HostedFixture::openai_missing_store_details()),
        KitError::new(
            ErrorKind::Timeout,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(HostedFixture::openai_missing_store_details()),
        KitError::new(
            ErrorKind::InvalidResponse(ResponseViolation::MalformedJson),
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(HostedFixture::openai_missing_store_details()),
    ] {
        assert!(
            HostedEvidence::missing_store(
                &error,
                Operation::GenerateStream,
                HostedProvider::OpenAi,
                HostedFixture::MISSING_OPENAI_STORE,
            )
            .is_err()
        );
    }
    for (provider, operation) in [
        (HostedProvider::Gemini, Operation::GenerateStream),
        (HostedProvider::Anthropic, Operation::GenerateStream),
        (HostedProvider::OpenAi, Operation::Generate),
    ] {
        assert!(
            HostedEvidence::missing_store(
                &observed,
                operation,
                provider,
                HostedFixture::MISSING_OPENAI_STORE,
            )
            .is_err()
        );
    }
}

#[test]
fn hosted_answers_accept_a_single_value_with_separate_prose_and_citations() {
    for text in [
        "Answer: IANA",
        "Answer: IANA.",
        "The page identifies the authority below.\r\n  Answer: IANA.  \r\nSource: https://www.iana.org/help/example-domains",
    ] {
        let mut response = HostedFixture::response(Vec::new());
        response.message = Message::assistant_text(text).unwrap();
        HostedEvidence::assess(&response, Some(HostedAnswer::Organization("IANA")), Ok(()))
            .unwrap();
    }
    let mut response = HostedFixture::response(Vec::new());
    response.message = Message::assistant_text(
        "Answer: ai-kit-provider-strict-file-search-v1.\nThe stored document supplies this marker.",
    )
    .unwrap();
    HostedEvidence::assess(
        &response,
        Some(HostedAnswer::Marker(
            "ai-kit-provider-strict-file-search-v1",
        )),
        Ok(()),
    )
    .unwrap();
}

#[test]
fn hosted_answers_accept_the_observed_multipart_iana_answer() {
    let prose = "\nThe user wants to find \"Further Reading\" section, then find the link title ending in \"-managed Reserved Domains\".\nFrom the browsed content:\nUnder \"Further Reading\", there is a link: \"IANA-managed Reserved Domains\".\nThe abbreviation immediately before \"-managed\" is \"IANA\".\n\nSo the answer is IANA.";
    let mut response = HostedFixture::response(Vec::new());
    response.message = Message::assistant(
        vec![
            ContentPart::Text { text: prose.into() },
            ContentPart::Text {
                text: "Answer: IANA".into(),
            },
        ],
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        GenerationEvidence::text(&response).unwrap(),
        format!("{prose}Answer: IANA"),
        "answer parsing must not insert a separator into portable text"
    );
    HostedEvidence::assess(&response, Some(HostedAnswer::Organization("IANA")), Ok(()))
        .expect("the observed final text part contains one complete correct Answer field");
}

#[test]
fn hosted_answers_accept_fields_split_across_text_parts() {
    for parts in [
        vec!["Ans", "wer: IA", "NA"],
        vec!["Answer:", " IANA"],
        vec!["Prose.\n ", "Answer: IANA"],
        vec!["So the answer is IANA.", "Ans", "wer: IA", "NA"],
        vec!["Answer: IA", "NA.", "\nSource: the requested page"],
    ] {
        let mut response = HostedFixture::response(Vec::new());
        response.message = Message::assistant(
            parts
                .iter()
                .map(|part| ContentPart::Text {
                    text: (*part).into(),
                })
                .collect(),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(GenerationEvidence::text(&response).unwrap(), parts.concat());
        HostedEvidence::assess(&response, Some(HostedAnswer::Organization("IANA")), Ok(()))
            .unwrap_or_else(|error| panic!("split field {parts:?} was rejected: {error}"));
    }
}

#[test]
fn hosted_answers_reject_duplicate_or_wrong_fields_at_text_part_starts() {
    for parts in [
        vec!["Prose.", "Answer: IANA\n", "Answer: IANA"],
        vec!["Prose.", "Answer: ICANN\n", "Answer: IANA"],
        vec!["Answer: IANA\n", "Prose.", "Answer: ICANN"],
        vec!["Answer: IANA\n", "Prose.", "Answer: IANA"],
        vec!["Prose.", "Answer: IANA", "Answer: IANA"],
        vec!["So the answer is IANA.", "Answer: ICANN"],
    ] {
        let mut response = HostedFixture::response(Vec::new());
        response.message = Message::assistant(
            parts
                .iter()
                .map(|part| ContentPart::Text {
                    text: (*part).into(),
                })
                .collect(),
            Vec::new(),
        )
        .unwrap();
        assert!(
            HostedEvidence::assess(&response, Some(HostedAnswer::Organization("IANA")), Ok(()))
                .is_err(),
            "all answer fields must count, including incorrect or duplicate fields at part starts: {parts:?}"
        );
    }
}

#[test]
fn hosted_answers_reject_decorations_and_suffixes_split_across_text_parts() {
    for parts in [
        vec![
            "Prose.",
            "Answer: IANA",
            " [source](https://www.iana.org/help/example-domains)",
        ],
        vec!["Answer: IANA", " [citation]"],
        vec!["Answer: IANA.", "."],
        vec!["Answer: IA", "NA-extra"],
        vec!["Prose.", "Answer: IA", "NA and another authority"],
        vec!["Answer: IANA", " more prose without a newline"],
    ] {
        let mut response = HostedFixture::response(Vec::new());
        response.message = Message::assistant(
            parts
                .iter()
                .map(|part| ContentPart::Text {
                    text: (*part).into(),
                })
                .collect(),
            Vec::new(),
        )
        .unwrap();
        assert!(
            HostedEvidence::assess(&response, Some(HostedAnswer::Organization("IANA")), Ok(()))
                .is_err(),
            "part boundaries cannot hide answer decoration or suffixes: {parts:?}"
        );
    }
}

#[test]
fn hosted_answers_preserve_verbatim_multipart_stream_agreement() {
    let mut response = HostedFixture::response(Vec::new());
    response.message = Message::assistant(
        vec![
            ContentPart::Text {
                text: "So the answer is IANA.".into(),
            },
            ContentPart::Text {
                text: "Answer: IANA".into(),
            },
        ],
        Vec::new(),
    )
    .unwrap();
    GenerationEvidence::exact_text(&response, "So the answer is IANA.Answer: IANA", false).unwrap();
    for separator in ["", "\n"] {
        let mut stream = StreamEvidence::new();
        stream
            .observe(GenerateEvent::Started {
                model: response.model.clone(),
            })
            .unwrap();
        for delta in ["So the answer is IANA.", separator, "Ans", "wer: IA", "NA"] {
            stream
                .observe(GenerateEvent::ContentDelta {
                    delta: delta.into(),
                })
                .unwrap();
        }
        let completion = stream.observe(GenerateEvent::Completed(Box::new(response.clone())));
        if separator.is_empty() {
            completion.unwrap();
            let completed = stream.finish().unwrap();
            assert_eq!(completed.message.content(), response.message.content());
        } else {
            assert!(
                completion.is_err(),
                "inserting a newline into deltas must still contradict completed text"
            );
        }
    }
}

#[test]
fn hosted_answers_reject_incidental_tokens_ambiguous_fields_and_inline_decoration() {
    for text in [
        "IANA",
        "The answer is not IANA.",
        "answer: IANA",
        "Answer:",
        "Answer: IANA\nAnswer: IANA",
        "Answer: IANA\nAnswer: ICANN",
        "Answer: IANA-like",
        "Answer: **IANA**",
        "Answer: IANA..",
        "Answer: IANA [source](https://www.iana.org/help/example-domains)",
        "Answer: not IANA",
        "Answer: IANA and another authority",
        "Answer: ICANN",
        "Answer: PTI\nThe documentation title is IANA-managed Reserved Domains.",
    ] {
        let mut response = HostedFixture::response(Vec::new());
        response.message = Message::assistant_text(text).unwrap();
        assert!(
            HostedEvidence::assess(&response, Some(HostedAnswer::Organization("IANA")), Ok(()))
                .is_err(),
            "an incidental token or ambiguous answer must not pass: {text:?}"
        );
    }
    for text in [
        "Answer: ai-kit-provider-strict-file-search-v10",
        "Answer: prefix-ai-kit-provider-strict-file-search-v1",
        "Answer: ai-kit-provider-strict-file-search-v1-extra",
    ] {
        let mut response = HostedFixture::response(Vec::new());
        response.message = Message::assistant_text(text).unwrap();
        assert!(
            HostedEvidence::assess(
                &response,
                Some(HostedAnswer::Marker(
                    "ai-kit-provider-strict-file-search-v1"
                )),
                Ok(()),
            )
            .is_err()
        );
    }
}

#[test]
fn hosted_integer_answers_require_a_complete_decimal_value() {
    let expected = (1_u64..=100).map(|value| value * value).sum::<u64>();
    assert_eq!(expected, 338350);
    for text in [
        "Answer: 338350",
        "The hosted runtime returned:\nAnswer: 338350\nComputation complete.",
    ] {
        let mut response = HostedFixture::response(Vec::new());
        response.message = Message::assistant_text(text).unwrap();
        HostedEvidence::assess(&response, Some(HostedAnswer::Integer(expected)), Ok(())).unwrap();
    }
    for text in [
        "Answer: 3383500",
        "Answer: 338,350",
        "Answer: +338350",
        "Answer: -338350",
        "Answer: 338350.0",
        "Answer: 338350.",
        "Answer: 3.38350e5",
        "Answer: ３３８３５０",
        "Answer: 18446744073709551616",
        "The number 338350 is not the result.",
    ] {
        let mut response = HostedFixture::response(Vec::new());
        response.message = Message::assistant_text(text).unwrap();
        assert!(
            HostedEvidence::assess(&response, Some(HostedAnswer::Integer(expected)), Ok(()))
                .is_err()
        );
    }
}

#[test]
fn hosted_assessment_preserves_independent_completion_answer_and_evidence_failures() {
    let mut response = HostedFixture::response(Vec::new());
    response.message = Message::assistant_text("Answer: WRONG").unwrap();
    response.finish_reason = Some(FinishReason::Length);
    let error = HostedEvidence::assess(
        &response,
        Some(HostedAnswer::Organization("IANA")),
        HostedEvidence::file_search(
            &response,
            HostedProvider::OpenAi,
            FileSearchContract::Exclusive,
        ),
    )
    .unwrap_err();
    for dimension in [
        "generation-completion:",
        "answer-incorrect:",
        "hosted-evidence:",
        "results=omitted",
        "file_citations=0",
    ] {
        assert!(error.contains(dimension), "missing {dimension}: {error}");
    }
}

#[test]
fn missing_store_fixture_uses_one_validated_nonce_without_length_inflation() {
    let nonce = "strict-0123456789abcdef0123456789abcdef";
    let gemini = HostedEvidence::missing_store_id(HostedProvider::Gemini, nonce).unwrap();
    assert_eq!(gemini, format!("fileSearchStores/{nonce}"));
    assert_eq!(gemini.strip_prefix("fileSearchStores/").unwrap().len(), 39);
    assert_eq!(
        HostedEvidence::missing_store_id(HostedProvider::OpenAi, nonce).unwrap(),
        format!("vs_{nonce}")
    );
    assert!(HostedEvidence::missing_store_id(HostedProvider::Anthropic, nonce).is_err());
    for invalid in [
        "strict-",
        "strict-0123456789abcdef0123456789abcde",
        "strict-0123456789abcdef0123456789abcdef0",
        "strict-0123456789ABCDEF0123456789ABCDEF",
        "strictstrict-0123456789abcdef0123456789abcdef",
        "strict-0123456789abcdef0123456789abcdeg",
        "0123456789abcdef0123456789abcdef",
    ] {
        assert!(HostedEvidence::missing_store_id(HostedProvider::Gemini, invalid).is_err());
    }
}

#[test]
fn failure_sources_accept_quote_equivalence_but_reject_extra_execution() {
    for (provider, source, declaration_status, result_status, exit_code) in [
        (
            HostedProvider::Anthropic,
            "python3 -c \"raise RuntimeError('EXPECTED_SENTINEL')\"",
            Some(HostedToolStatus::InProgress),
            Some(HostedFixture::failure()),
            Some(1),
        ),
        (
            HostedProvider::Gemini,
            "raise RuntimeError('EXPECTED_SENTINEL')",
            None,
            Some(HostedFixture::failure()),
            None,
        ),
    ] {
        let mut declaration = HostedFixture::failure_code(declaration_status.clone());
        if let HostedToolOutput::CodeExecution { code, .. } = &mut declaration {
            code[0].code = format!("\n{source}\n");
        }
        let result = HostedFixture::result(
            Some(HostedToolStatus::Completed),
            result_status,
            "RuntimeError: EXPECTED_SENTINEL\n",
            exit_code,
        );
        let result = if matches!(provider, HostedProvider::Anthropic) {
            HostedFixture::stderr(result)
        } else {
            result
        };
        let output = HostedFixture::paired(provider, declaration, result);
        HostedEvidence::code_failure(
            &HostedFixture::response(vec![output]),
            provider,
            "EXPECTED_SENTINEL",
        )
        .unwrap();
    }
    for source in [
        "print('RuntimeError: EXPECTED_SENTINEL')",
        "raise RuntimeError(\"EXPECTED_SENTINEL\"); print('extra')",
        "try:\n raise RuntimeError(\"EXPECTED_SENTINEL\")\nexcept RuntimeError:\n pass",
        "python -c 'raise RuntimeError(\"EXPECTED_SENTINEL\")'",
    ] {
        let mut record = HostedFixture::openai_failure_record("RuntimeError: EXPECTED_SENTINEL\n");
        if let HostedToolOutput::CodeExecution { code, .. } = &mut record {
            code[0].code = source.into();
        }
        assert!(
            HostedEvidence::code_failure(
                &HostedFixture::response(vec![record]),
                HostedProvider::OpenAi,
                "EXPECTED_SENTINEL"
            )
            .is_err()
        );
    }
    assert!(HostedEvidence::failure_source(HostedProvider::Anthropic, "unsafe'; shell").is_err());
}

#[test]
fn execution_logs_do_not_accept_cursor_controls_or_a_different_exception() {
    for logs in [
        "RuntimeError: EXPECTED_SENTINEL_suffix\n",
        "RuntimeError: prefix_EXPECTED_SENTINEL\n",
        "\u{1b}[2JRuntimeError: EXPECTED_SENTINEL\n",
        "\u{1b}[31RuntimeError: EXPECTED_SENTINEL\n",
        "unrelated\u{7}line\nRuntimeError: EXPECTED_SENTINEL\n",
        "unrelated\rline\nRuntimeError: EXPECTED_SENTINEL\n",
        "unrelated\u{8}line\nRuntimeError: EXPECTED_SENTINEL\n",
        "unrelated\u{7f}line\nRuntimeError: EXPECTED_SENTINEL\n",
        "The tool says RuntimeError: EXPECTED_SENTINEL\n",
    ] {
        let response = HostedFixture::response(vec![HostedFixture::openai_failure_record(logs)]);
        assert!(
            HostedEvidence::code_failure(&response, HostedProvider::OpenAi, "EXPECTED_SENTINEL")
                .is_err()
        );
    }
}

#[test]
fn execution_exception_lines_allow_horizontal_whitespace_and_crlf() {
    for logs in [
        "RuntimeError:\tEXPECTED_SENTINEL\r\n",
        "\tRuntimeError: \t EXPECTED_SENTINEL\t\n",
        "\u{1b}[1;31mRuntimeError:\u{1b}[0m\tEXPECTED_SENTINEL\r\n",
    ] {
        let response = HostedFixture::response(vec![HostedFixture::openai_failure_record(logs)]);
        HostedEvidence::code_failure(&response, HostedProvider::OpenAi, "EXPECTED_SENTINEL")
            .unwrap();
    }
}

#[test]
fn gemini_execution_error_requires_outcome_failed_without_an_invented_exit_code() {
    for (code, exit_code) in [
        ("OUTCOME_DEADLINE_EXCEEDED", None),
        ("UNRELATED_FAILURE", None),
        ("OUTCOME_FAILED", Some(1)),
        ("OUTCOME_FAILED", Some(0)),
    ] {
        let status = HostedToolStatus::Failed {
            error: RemoteFailure::new(
                ErrorKind::ProviderRejected,
                NativeErrorDetails {
                    outcome: Some(code.into()),
                    ..NativeErrorDetails::default()
                },
            )
            .unwrap(),
        };
        let response = HostedFixture::response(vec![HostedFixture::paired(
            HostedProvider::Gemini,
            HostedFixture::failure_code(None),
            HostedFixture::result(
                Some(status.clone()),
                Some(status),
                "RuntimeError: EXPECTED_SENTINEL\n",
                exit_code,
            ),
        )]);
        assert!(
            HostedEvidence::code_failure(&response, HostedProvider::Gemini, "EXPECTED_SENTINEL")
                .is_err()
        );
    }
}

#[test]
fn anthropic_exception_proof_requires_stderr_and_nonzero_exit() {
    let mut declaration = HostedFixture::failure_code(Some(HostedToolStatus::InProgress));
    if let HostedToolOutput::CodeExecution { code, .. } = &mut declaration {
        code[0].code =
            HostedEvidence::failure_source(HostedProvider::Anthropic, "EXPECTED_SENTINEL").unwrap();
    }
    for result in [
        HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedFixture::failure()),
            "RuntimeError: EXPECTED_SENTINEL\n",
            Some(1),
        ),
        HostedFixture::stderr(HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedFixture::failure()),
            "RuntimeError: EXPECTED_SENTINEL\n",
            Some(0),
        )),
        HostedFixture::stderr(HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedFixture::failure()),
            "RuntimeError: EXPECTED_SENTINEL\n",
            None,
        )),
    ] {
        let response = HostedFixture::response(vec![HostedFixture::paired(
            HostedProvider::Anthropic,
            declaration.clone(),
            result,
        )]);
        assert!(
            HostedEvidence::code_failure(&response, HostedProvider::Anthropic, "EXPECTED_SENTINEL")
                .is_err()
        );
    }
}

#[test]
fn gemini_combined_execution_record_cannot_invent_a_declaration_status() {
    let mut record = HostedFixture::openai_failure_record("RuntimeError: EXPECTED_SENTINEL\n");
    if let HostedToolOutput::CodeExecution { results, .. } = &mut record {
        results[0].status = Some(HostedFixture::failure());
    }
    assert!(
        HostedEvidence::code_failure(
            &HostedFixture::response(vec![record]),
            HostedProvider::Gemini,
            "EXPECTED_SENTINEL"
        )
        .is_err()
    );
}

#[test]
fn separate_declaration_cannot_be_consumed_by_multiple_results() {
    for provider in [HostedProvider::Anthropic, HostedProvider::Gemini] {
        let declaration = HostedFixture::code(None);
        let mut result = HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedToolStatus::Completed),
            "338350\n",
            None,
        );
        if let HostedToolOutput::CodeExecution { results, .. } = &mut result {
            results.push(results[0].clone());
        }
        let response = HostedFixture::response(vec![declaration, result]);
        assert!(HostedEvidence::code_success(&response, provider, "338350").is_err());
    }
}

#[test]
fn resolution_web_search_cannot_borrow_sources_from_an_unfinished_record() {
    let pending = HostedFixture::web(Some(HostedToolStatus::InProgress));
    let mut completed = HostedFixture::web(Some(HostedToolStatus::Completed));
    if let HostedToolOutput::WebSearch {
        action,
        results,
        citations,
        ..
    } = &mut completed
    {
        *action = None;
        results.clear();
        citations.clear();
    }
    assert!(
        HostedEvidence::web_search(
            &HostedFixture::response(vec![pending, completed]),
            HostedProvider::OpenAi
        )
        .is_err()
    );
}

#[test]
fn resolution_file_search_cannot_borrow_results_from_an_unfinished_record() {
    let mut pending = HostedFixture::file();
    if let HostedToolOutput::FileSearch { status, .. } = &mut pending {
        *status = Some(HostedToolStatus::InProgress);
    }
    let mut completed = HostedFixture::file();
    if let HostedToolOutput::FileSearch {
        results, citations, ..
    } = &mut completed
    {
        *results = Some(Vec::new());
        citations.clear();
    }
    assert!(
        HostedEvidence::file_search(
            &HostedFixture::response(vec![pending, completed]),
            HostedProvider::OpenAi,
            FileSearchContract::Exclusive,
        )
        .is_err()
    );
}

#[test]
fn resolution_openai_web_search_query_is_optional_when_sources_prove_the_action() {
    let mut value =
        serde_json::to_value(HostedFixture::web(Some(HostedToolStatus::Completed))).unwrap();
    value["action"]["type"] = serde_json::json!("search");
    value["action"]["query"] = serde_json::Value::Null;
    value["action"]["queries"] = serde_json::json!([]);
    let output = serde_json::from_value(value).unwrap();
    HostedEvidence::web_search(
        &HostedFixture::response(vec![output]),
        HostedProvider::OpenAi,
    )
    .unwrap();
}

#[test]
fn resolution_openai_open_page_and_find_actions_do_not_require_search_queries() {
    for kind in ["open_page", "find_in_page"] {
        let mut value =
            serde_json::to_value(HostedFixture::web(Some(HostedToolStatus::Completed))).unwrap();
        value["action"] = if kind == "open_page" {
            serde_json::json!({"type":kind,"url":HostedFixture::URL})
        } else {
            serde_json::json!({"type":kind,"url":HostedFixture::URL,"pattern":"example domains"})
        };
        value["results"] = serde_json::json!([]);
        let output = serde_json::from_value(value).unwrap();
        HostedEvidence::web_search(
            &HostedFixture::response(vec![output]),
            HostedProvider::OpenAi,
        )
        .unwrap();
    }
}

#[test]
fn resolution_gemini_url_success_uses_retrieval_identity_without_a_fake_snippet() {
    let output = serde_json::from_value(serde_json::json!({
        "family":"url_context", "status":{"state":"completed"},
        "requested_url":null, "retrieved_url":HostedFixture::URL, "citations":[]
    }))
    .unwrap();
    HostedEvidence::url_context(
        &HostedFixture::response(vec![output]),
        HostedProvider::Gemini,
        HostedFixture::URL,
    )
    .unwrap();
}

#[test]
fn resolution_url_failure_cannot_be_proven_by_unrelated_or_ineligible_activity() {
    for (url, kind, code) in [
        (
            "https://unrelated.invalid/",
            ErrorKind::ProviderRejected,
            "url_not_accessible",
        ),
        (
            HostedFixture::URL,
            ErrorKind::RateLimited,
            "too_many_requests",
        ),
        (
            HostedFixture::URL,
            ErrorKind::PermissionDenied,
            "url_not_allowed",
        ),
        (
            HostedFixture::URL,
            ErrorKind::InvalidRequest,
            "invalid_tool_input",
        ),
    ] {
        let failure = HostedToolStatus::Failed {
            error: RemoteFailure::new(
                kind,
                NativeErrorDetails {
                    code: Some(code.into()),
                    ..Default::default()
                },
            )
            .unwrap(),
        };
        let output = serde_json::from_value(serde_json::json!({
            "family":"url_context", "status":failure,
            "requested_url":url,"retrieved_url":null,"citations":[]
        }))
        .unwrap();
        assert!(
            HostedEvidence::url_failure(
                &HostedFixture::response(vec![output]),
                HostedProvider::Anthropic,
                HostedFixture::URL
            )
            .is_err()
        );
    }
}

#[test]
fn resolution_anthropic_code_success_requires_a_reported_zero_exit() {
    let response = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::Anthropic,
        HostedFixture::code(Some(HostedToolStatus::InProgress)),
        HostedFixture::stdout(HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedToolStatus::Completed),
            "338350\n",
            None,
        )),
    )]);
    assert!(HostedEvidence::code_success(&response, HostedProvider::Anthropic, "338350").is_err());
}

#[test]
fn resolution_gemini_code_success_rejects_an_invented_process_exit() {
    let response = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::Gemini,
        HostedFixture::code(None),
        HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedToolStatus::Completed),
            "338350\n",
            Some(0),
        ),
    )]);
    assert!(HostedEvidence::code_success(&response, HostedProvider::Gemini, "338350").is_err());
}

#[test]
fn resolution_openai_execution_uses_native_logs_without_invented_stdout() {
    for (text, negative) in [
        ("338350\n", false),
        ("RuntimeError: EXPECTED_SENTINEL\n", true),
    ] {
        let mut value = serde_json::to_value(HostedFixture::openai_failure_record(text)).unwrap();
        if !negative {
            value["code"][0]["code"] = serde_json::json!("print(sum(i*i for i in range(1, 101)))");
        }
        value["results"][0]["stdout"] = serde_json::Value::Null;
        value["results"][0]["logs"] = serde_json::json!(text);
        let response = HostedFixture::response(vec![serde_json::from_value(value).unwrap()]);
        if negative {
            HostedEvidence::code_failure(&response, HostedProvider::OpenAi, "EXPECTED_SENTINEL")
                .unwrap();
        } else {
            HostedEvidence::code_success(&response, HostedProvider::OpenAi, "338350").unwrap();
        }
    }
}

#[test]
fn resolution_code_success_reports_independent_status_and_exit_failures() {
    let response = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::Anthropic,
        HostedFixture::code(Some(HostedToolStatus::InProgress)),
        HostedFixture::result(
            Some(HostedFixture::failure()),
            Some(HostedFixture::failure()),
            "338350\n",
            Some(7),
        ),
    )]);
    let error =
        HostedEvidence::code_success(&response, HostedProvider::Anthropic, "338350").unwrap_err();
    assert!(
        error.contains("failed record"),
        "missing declaration failure: {error}"
    );
    assert!(
        error.contains("nonzero exit"),
        "missing independent result failure: {error}"
    );
}

#[test]
fn resolution_web_grounding_matches_native_redirect_citations_without_guessing_destinations() {
    let mut output = HostedFixture::web(None);
    if let HostedToolOutput::WebSearch {
        results, citations, ..
    } = &mut output
    {
        results[0].url =
            "https://vertexaisearch.cloud.google.com/grounding-api-redirect/native-source".into();
        if let Citation::Url { url, .. } = &mut citations[0] {
            *url = results[0].url.clone();
        }
    }
    HostedEvidence::web_search(
        &HostedFixture::response(vec![output]),
        HostedProvider::Gemini,
    )
    .unwrap();
}

#[test]
fn resolution_web_grounding_rejects_unusable_source_and_citation_urls() {
    for source in [
        "not a URL",
        "javascript:alert(1)",
        "https://user:password@example.com/source",
    ] {
        let mut output = HostedFixture::web(Some(HostedToolStatus::Completed));
        if let HostedToolOutput::WebSearch {
            results, citations, ..
        } = &mut output
        {
            results[0].url = source.into();
            if let Citation::Url { url, .. } = &mut citations[0] {
                *url = source.into();
            }
        }
        assert!(
            HostedEvidence::web_search(
                &HostedFixture::response(vec![output]),
                HostedProvider::OpenAi
            )
            .is_err()
        );
    }
}

#[test]
fn resolution_web_citation_must_match_the_completed_operation_source() {
    let mut source_only = HostedFixture::web(Some(HostedToolStatus::Completed));
    if let HostedToolOutput::WebSearch { citations, .. } = &mut source_only {
        citations.clear();
    }
    let mut unrelated = HostedFixture::web(Some(HostedToolStatus::InProgress));
    if let HostedToolOutput::WebSearch { results, .. } = &mut unrelated {
        results[0].url = "https://unrelated.invalid/source".into();
    }
    assert!(
        HostedEvidence::web_search(
            &HostedFixture::response(vec![source_only, unrelated]),
            HostedProvider::OpenAi
        )
        .is_err()
    );
}

#[test]
fn anthropic_positive_code_requires_native_stdout_and_zero_exit_together() {
    let response = HostedFixture::response(vec![HostedFixture::paired(
        HostedProvider::Anthropic,
        HostedFixture::code(Some(HostedToolStatus::InProgress)),
        HostedFixture::stdout(HostedFixture::result(
            Some(HostedToolStatus::Completed),
            Some(HostedToolStatus::Completed),
            "338350\n",
            Some(0),
        )),
    )]);
    HostedEvidence::code_success(&response, HostedProvider::Anthropic, "338350").unwrap();
    let mut wrong_channel = response;
    if let HostedToolOutput::CodeExecution { results, .. } =
        &mut wrong_channel.hosted_tool_outputs[0]
    {
        results[0].logs = results[0].stdout.take();
    }
    assert!(
        HostedEvidence::code_success(&wrong_channel, HostedProvider::Anthropic, "338350").is_err()
    );
}

#[test]
fn gemini_url_error_requires_the_exact_retrieved_target_and_retrieval_outcome() {
    for (target, outcome, kind, accepted) in [
        (
            HostedFixture::URL,
            "URL_RETRIEVAL_STATUS_ERROR",
            ErrorKind::ProviderRejected,
            true,
        ),
        (
            "https://unrelated.invalid/",
            "URL_RETRIEVAL_STATUS_ERROR",
            ErrorKind::ProviderRejected,
            false,
        ),
        (
            HostedFixture::URL,
            "URL_RETRIEVAL_STATUS_PAYWALL",
            ErrorKind::PermissionDenied,
            false,
        ),
        (
            HostedFixture::URL,
            "URL_RETRIEVAL_STATUS_UNSAFE",
            ErrorKind::PermissionDenied,
            false,
        ),
        (
            HostedFixture::URL,
            "URL_RETRIEVAL_STATUS_UNSPECIFIED",
            ErrorKind::ProviderRejected,
            false,
        ),
        (
            HostedFixture::URL,
            "URL_RETRIEVAL_STATUS_ERROR",
            ErrorKind::Authentication,
            false,
        ),
    ] {
        let response = HostedFixture::response(vec![HostedToolOutput::UrlContext {
            status: Some(HostedToolStatus::Failed {
                error: RemoteFailure::new(
                    kind,
                    NativeErrorDetails {
                        outcome: Some(outcome.into()),
                        ..Default::default()
                    },
                )
                .unwrap(),
            }),
            requested_url: None,
            retrieved_url: Some(target.into()),
            citations: Vec::new(),
        }]);
        assert_eq!(
            HostedEvidence::url_failure(&response, HostedProvider::Gemini, HostedFixture::URL)
                .is_ok(),
            accepted
        );
    }
}

#[test]
fn url_success_does_not_infer_target_identity_from_citation_text() {
    for provider in [HostedProvider::Anthropic, HostedProvider::Gemini] {
        let response = HostedFixture::response(vec![HostedToolOutput::UrlContext {
            status: Some(HostedToolStatus::Completed),
            requested_url: None,
            retrieved_url: None,
            citations: vec![Citation::Url {
                url: HostedFixture::URL.into(),
                title: None,
                snippet: Some("URL_RETRIEVAL_STATUS_SUCCESS".into()),
                start_index: None,
                end_index: None,
            }],
        }]);
        assert!(HostedEvidence::url_context(&response, provider, HostedFixture::URL).is_err());
    }
}

#[test]
fn incomplete_search_and_code_activity_cannot_borrow_a_neighboring_success() {
    let web = HostedFixture::response(vec![
        HostedFixture::web(Some(HostedToolStatus::Completed)),
        HostedFixture::web(Some(HostedToolStatus::Incomplete)),
    ]);
    assert!(HostedEvidence::web_search(&web, HostedProvider::OpenAi).is_err());
    let mut success = HostedFixture::openai_failure_record("338350\n");
    if let HostedToolOutput::CodeExecution { code, .. } = &mut success {
        code[0].code = "print(sum(i*i for i in range(1, 101)))".into();
    }
    let mut incomplete = success.clone();
    if let HostedToolOutput::CodeExecution { status, .. } = &mut incomplete {
        *status = Some(HostedToolStatus::Incomplete);
    }
    assert!(
        HostedEvidence::code_success(
            &HostedFixture::response(vec![success, incomplete]),
            HostedProvider::OpenAi,
            "338350"
        )
        .is_err()
    );
}

#[test]
fn gemini_file_search_requires_included_retrieval_evidence_without_inventing_ids() {
    for (results, accepted) in [(None, false), (Some(Vec::new()), true)] {
        let response = HostedFixture::response(vec![HostedToolOutput::FileSearch {
            status: None,
            queries: Vec::new(),
            results,
            citations: Vec::new(),
        }]);
        assert_eq!(
            HostedEvidence::file_search(
                &response,
                HostedProvider::Gemini,
                FileSearchContract::Exclusive
            )
            .is_ok(),
            accepted
        );
    }
}

#[test]
fn code_failure_retains_expected_source_when_native_code_and_logs_agree_on_wrong_marker() {
    let expected = "STRICT_HOSTED_FAILURE_strict-00000000000040008000000000000001";
    for returned in [
        expected,
        "STRICT_HOSTED_FAILURE_strict-00000000000040008000000000000002",
        "STRICT_HOSTED_FAILURE_strict-11111111111141118111111111111111",
        "",
    ] {
        let source = HostedToolOutput::CodeExecution {
            status: None,
            code: vec![HostedExecutableCode {
                language: Some("PYTHON".into()),
                code: format!("raise RuntimeError(\"{returned}\")\n"),
            }],
            results: Vec::new(),
            artifacts: Vec::new(),
        };
        let response = HostedFixture::response(vec![HostedFixture::paired(
            HostedProvider::Gemini,
            source,
            HostedFixture::result(
                None,
                Some(HostedFixture::failure()),
                &format!("Traceback (most recent call last):\nRuntimeError: {returned}\n"),
                None,
            ),
        )]);
        let result = HostedEvidence::code_failure(&response, HostedProvider::Gemini, expected);
        if returned == expected {
            result.expect("the independently supplied marker must pass with paired native failure");
        } else {
            let diagnostic = result.expect_err(
                "agreement between returned code and logs cannot replace the expected marker",
            );
            assert!(diagnostic.contains("paired_sources=1"));
            assert!(diagnostic.contains("matching_sources=0"));
            assert!(diagnostic.contains("exception_logs=0"));
            assert!(diagnostic.contains("native_failure_results=1"));
            assert!(
                diagnostic.contains(
                    r#"expected_source="raise RuntimeError(\"STRICT_HOSTED_FAILURE_strict-00000000000040008000000000000001\")""#
                ),
                "the failure must preserve the independent requested source: {diagnostic}"
            );
        }
    }
}

#[test]
fn code_failure_reports_independent_declaration_pairing_and_result_evidence() {
    for invented_declaration in [false, true] {
        let mut output = HostedFixture::paired(
            HostedProvider::Gemini,
            HostedFixture::failure_code(None),
            HostedFixture::result(
                None,
                Some(HostedToolStatus::Completed),
                "unrelated output",
                None,
            ),
        );
        if let HostedToolOutput::CodeExecution { status, code, .. } = &mut output {
            *status = invented_declaration.then_some(HostedToolStatus::Completed);
            code.push(code[0].clone());
        }
        let diagnostic = HostedEvidence::code_failure(
            &HostedFixture::response(vec![output]),
            HostedProvider::Gemini,
            "EXPECTED_SENTINEL",
        )
        .unwrap_err();
        if invented_declaration {
            assert!(diagnostic.contains("executableCode does not report a lifecycle status"));
        }
        assert!(diagnostic.contains("exactly one source"));
        assert!(diagnostic.contains("paired_sources=0"));
        assert!(diagnostic.contains("exception_logs=0"));
        assert!(diagnostic.contains("native_failure_results=0"));
    }
}

#[test]
fn qualified_capture_retains_declaration_before_missing_text_failure() {
    for stage in [
        FileSearchStage::Retrieval,
        FileSearchStage::Preflight,
        FileSearchStage::PostDelete,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut response =
            HostedFixture::gemini_file_search_with_query("print(file_search.query(\"fixture\"))");
        response.message = Message::assistant(
            vec![ContentPart::Thinking {
                text: Some("retrieval thought".into()),
            }],
            Vec::new(),
        )
        .unwrap();
        let expected = CaseFailure::response_evidence(&response);
        let mut stream = HostedFixture::captured_stream(directory.path(), stage);
        stream
            .observe_result(Ok(GenerateEvent::Started {
                model: response.model.clone(),
            }))
            .unwrap();
        stream
            .observe_result(Ok(GenerateEvent::Completed(Box::new(response))))
            .unwrap();
        assert!(stream.take_export_failures().is_empty());
        assert!(
            stream
                .finish_observation()
                .unwrap_err()
                .to_string()
                .contains("actual text deltas")
        );
        let captures = HostedFixture::captures(directory.path());
        assert_eq!(
            captures.len(),
            1,
            "a failed stream must retain its mandatory complete response"
        );
        assert_eq!(captures[0]["response"], expected);
        assert_eq!(captures[0]["stage"], stage.label());
        assert_eq!(captures[0]["stream_acceptance"], "not_established");
        assert_eq!(captures[0]["retrieval_acceptance"], "not_established");
        assert_eq!(
            captures[0]["strict_file_search_only_requirement_met"],
            false
        );
        assert_eq!(captures[0]["code_execution_claimed"], false);
        assert_eq!(captures[0]["retrieval_revocation_proven"], false);
    }
}

#[test]
fn qualified_capture_retains_completions_rejected_during_observation() {
    for mutation in ["model", "text", "duplicate", "before_start"] {
        let directory = tempfile::tempdir().unwrap();
        let mut response =
            HostedFixture::gemini_file_search_with_query("print(file_search.query(\"fixture\"))");
        let mut stream =
            HostedFixture::captured_stream(directory.path(), FileSearchStage::Retrieval);
        if mutation != "before_start" {
            stream
                .observe_result(Ok(GenerateEvent::Started {
                    model: response.model.clone(),
                }))
                .unwrap();
            stream
                .observe_result(Ok(GenerateEvent::ContentDelta {
                    delta: "Answer: fixture-secret".into(),
                }))
                .unwrap();
        }
        match mutation {
            "model" => response.model = ModelId::new("unexpected-model").unwrap(),
            "text" => response.message = Message::assistant_text("mismatched text").unwrap(),
            "duplicate" => stream
                .observe_result(Ok(GenerateEvent::Completed(Box::new(response.clone()))))
                .unwrap(),
            _ => {}
        }
        let expected = CaseFailure::response_evidence(&response);
        assert!(
            stream
                .observe_result(Ok(GenerateEvent::Completed(Box::new(response))))
                .is_err()
        );
        assert!(stream.take_export_failures().is_empty());
        let captures = HostedFixture::captures(directory.path());
        assert_eq!(
            captures.len(),
            if mutation == "duplicate" { 2 } else { 1 },
            "capture must precede {mutation} validation"
        );
        assert_eq!(captures.last().unwrap()["response"], expected);
    }
}

#[test]
fn qualified_capture_preserves_success_and_redacts_only_persisted_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let response = HostedFixture::gemini_file_search_with_query(
        "print(file_search.query(\"private-capture-credential\"))",
    );
    let original = CaseFailure::response_evidence(&response);
    let mut stream = HostedFixture::captured_stream(directory.path(), FileSearchStage::Retrieval);
    stream
        .observe_result(Ok(GenerateEvent::Started {
            model: response.model.clone(),
        }))
        .unwrap();
    stream
        .observe_result(Ok(GenerateEvent::ContentDelta {
            delta: GenerationEvidence::text(&response).unwrap(),
        }))
        .unwrap();
    stream
        .observe_result(Ok(GenerateEvent::Completed(Box::new(response))))
        .unwrap();
    assert!(stream.take_export_failures().is_empty());
    let completed = stream.finish_observation().unwrap();
    HostedFixture::qualified_file_search(&completed).unwrap();
    assert_eq!(CaseFailure::response_evidence(&completed), original);
    let mut redacted = original;
    CaseFailure::redact_credentials(&mut redacted, &["private-capture-credential".into()]);
    let captures = HostedFixture::captures(directory.path());
    assert_eq!(captures.len(), 1);
    assert_eq!(captures[0]["response"], redacted);
    assert!(
        !captures[0]
            .to_string()
            .contains("private-capture-credential")
    );
}

#[test]
fn qualified_capture_export_failures_survive_every_operation_exit_and_cleanup() {
    for exit in [
        "missing_text",
        "sdk_before_completion",
        "sdk_after_completion",
        "timeout",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let bad_destination = directory.path().join("not-a-directory");
        std::fs::write(&bad_destination, b"keep-existing-evidence").unwrap();
        let mut stream =
            HostedFixture::captured_stream(&bad_destination, FileSearchStage::Preflight);
        let mut response =
            HostedFixture::gemini_file_search_with_query("print(file_search.query(\"fixture\"))");
        response.message = Message::assistant(
            vec![ContentPart::Thinking {
                text: Some("retrieval thought".into()),
            }],
            Vec::new(),
        )
        .unwrap();
        stream
            .observe_result(Ok(GenerateEvent::Started {
                model: response.model.clone(),
            }))
            .unwrap();
        let error = KitError::new(
            ErrorKind::Transport,
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_native_details(NativeErrorDetails {
            message: Some("retained-native-error".into()),
            ..Default::default()
        });
        if exit == "sdk_before_completion" {
            stream.observe_result(Err(error.clone())).unwrap();
        }
        let observed = stream.observe_result(Ok(GenerateEvent::Completed(Box::new(response))));
        // LiveProvider drains immediately after its timed future, before deciding
        // which operation outcome to return and before resource/shutdown cleanup.
        let exports = stream.take_export_failures();
        assert_eq!(exports.len(), 1);
        assert!(stream.take_export_failures().is_empty());
        let primary = match exit {
            "sdk_before_completion" => observed,
            "sdk_after_completion" => {
                observed.unwrap();
                stream.observe_result(Err(error))
            }
            "timeout" => {
                observed.unwrap();
                Err(stream.interrupted("stream EOF deadline expired"))
            }
            _ => {
                observed.unwrap();
                stream.finish_observation().map(|_| ())
            }
        };
        assert!(primary.is_err());
        let combined = CaseFailure::combine(
            CaseFailure::combine(primary, Err("owned cleanup failed".into())),
            CaseFailure::combine(
                Err("shutdown failed".into()),
                CaseFailure::collected("capture exports", exports),
            ),
        )
        .unwrap_err();
        let retained = combined.evidence().to_string();
        assert!(retained.contains("required File Search response capture failed"));
        assert!(retained.contains("owned cleanup failed"));
        assert!(retained.contains("shutdown failed"));
        let expected = match exit {
            "sdk_before_completion" | "sdk_after_completion" => "retained-native-error",
            "timeout" => "stream EOF deadline expired",
            _ => "actual text deltas",
        };
        assert!(
            retained.contains(expected),
            "lost primary failure for {exit}"
        );
        assert!(!format!("{combined}").contains("retained-native-error"));
        assert_eq!(
            std::fs::read(bad_destination).unwrap(),
            b"keep-existing-evidence"
        );
    }
}

#[test]
fn qualified_capture_rejects_oversized_evidence_without_a_truncated_substitute() {
    let directory = tempfile::tempdir().unwrap();
    let mut stream = HostedFixture::captured_stream(directory.path(), FileSearchStage::PostDelete);
    let response = HostedFixture::gemini_file_search_with_query(&format!(
        "print(file_search.query(\"{}\"))",
        "x".repeat(1024 * 1024)
    ));
    stream
        .observe_result(Ok(GenerateEvent::Started {
            model: response.model.clone(),
        }))
        .unwrap();
    stream
        .observe_result(Ok(GenerateEvent::ContentDelta {
            delta: GenerationEvidence::text(&response).unwrap(),
        }))
        .unwrap();
    stream
        .observe_result(Ok(GenerateEvent::Completed(Box::new(response))))
        .unwrap();
    let exports = stream.take_export_failures();
    assert_eq!(exports.len(), 1);
    let operation = stream.finish_observation().map(|_| ());
    assert!(operation.is_ok());
    assert!(
        CaseFailure::combine(
            operation,
            CaseFailure::collected("capture exports", exports)
        )
        .is_err()
    );
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn qualified_capture_uses_private_fallback_without_optional_evidence_configuration() {
    let directory = CaseFailure::required_observation_directory(None).unwrap();
    let expected_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../target/providers-kit-required-observations");
    assert!(directory.starts_with(expected_root));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    let mut stream = HostedFixture::captured_stream(&directory, FileSearchStage::Retrieval);
    let response =
        HostedFixture::gemini_file_search_with_query("print(file_search.query(\"fixture\"))");
    let expected = CaseFailure::response_evidence(&response);
    // Even a completion before Started must be preserved.
    assert!(
        stream
            .observe_result(Ok(GenerateEvent::Completed(Box::new(response))))
            .is_err()
    );
    assert!(stream.take_export_failures().is_empty());
    let captures = HostedFixture::captures(&directory);
    std::fs::remove_dir_all(&directory).unwrap();
    assert_eq!(captures.len(), 1);
    assert_eq!(captures[0]["response"], expected);
}

#[test]
fn qualified_capture_does_not_expand_unqualified_stream_acceptance() {
    for (contract, selected_model) in [
        (FileSearchContract::Exclusive, "models/gemini-2.5-flash"),
        (
            FileSearchContract::GeminiStreaming,
            "models/different-model",
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut stream = StreamEvidence::new().with_file_search_capture(FileSearchCapture {
            contract,
            selected_model: selected_model.into(),
            stage: FileSearchStage::Retrieval,
            directory: directory.path().into(),
            secrets: Vec::new(),
        });
        let response =
            HostedFixture::gemini_file_search_with_query("print(file_search.query(\"fixture\"))");
        assert!(
            stream
                .observe_result(Ok(GenerateEvent::Completed(Box::new(response))))
                .is_err()
        );
        assert!(stream.take_export_failures().is_empty());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}
