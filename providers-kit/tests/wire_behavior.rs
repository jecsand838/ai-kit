use providers_kit::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, JsonErrorCategory, Operation, ResponseDetails,
    ResponseViolation,
};
mod support;
mod wire_cases;

use futures_util::{StreamExt, TryStreamExt};
use providers_kit::batch::{BatchResult, BatchStatus, BatchSubmitRequest};
use providers_kit::config::{
    AnthropicConfig, ApiKey, GeminiConfig, OllamaConfig, OpenAiConfig, ProviderKind,
    TransportConfig,
};
use providers_kit::embedding::EmbedRequest;
use providers_kit::files::{FileListRequest, FileUploadRequest};
use providers_kit::generation::{
    Citation, ContentPart, FileReference, FinishReason, FunctionCall, FunctionResult,
    GenerateEvent, GenerateRequest, GenerationConfig, GenerationContinuation, Message,
    ProviderToolSpec, ReasoningConfig, ReasoningEffort, ReasoningSummaryMode, Usage,
};
use providers_kit::hosted::{HostedTool, HostedToolOutput};
use providers_kit::models::ModelListRequest;
use providers_kit::{
    KitError, Model, ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig,
};
use serde_json::{Value, json};
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use support::scripted_http::{RecordedRequest, ScriptedResponse, spawn_scripted_http_server};

struct OpaqueContinuation;

#[tokio::test]
async fn provider_http_reports_current_sdk_version() {
    let mut server =
        spawn_scripted_http_server(vec![ScriptedResponse::json(r#"{"data":[]}"#)]).await;
    let provider = WireProvider::OpenAi.build(server.base_url());
    let result = provider.list_models(ModelListRequest::default()).await;
    let shutdown = provider.shutdown().await;
    let settled = server.finish(Duration::from_secs(2)).await;
    assert!(result.is_ok(), "model response: {result:?}");
    assert!(shutdown.is_ok(), "provider cleanup: {shutdown:?}");
    assert_eq!(settled, Ok(()));
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].headers.get("user-agent"),
        Some(&format!("providers-kit/{}", env!("CARGO_PKG_VERSION")))
    );
}

#[tokio::test]
async fn scripted_http_drop_rejects_late_requests() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
        "{\"unexpected_late_effect\":true}",
    )])
    .await;
    let address = server.base_url().socket_addrs(|| None).unwrap()[0];
    drop(server);
    let observed = tokio::time::timeout(Duration::from_secs(2), async {
        let mut stream = match TcpStream::connect(address).await {
            Ok(stream) => stream,
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        };
        let request =
            b"GET /after-owner-drop HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        match stream.write_all(request).await {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        }
        let mut response = Vec::new();
        match stream.take(4096).read_to_end(&mut response).await {
            Ok(_) => Ok(response),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => Ok(response),
            Err(error) => Err(error),
        }
    })
    .await;
    // The one-response baseline task naturally finishes after serving its late
    // response. Observe listener closure before reporting that primary failure.
    // The original fixture API cannot establish an awaited task join.
    let released = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match TcpStream::connect(address).await {
                Ok(stream) => drop(stream),
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        matches!(&observed, Ok(Ok(response)) if response.is_empty()),
        "dropped server accepted a late request: observed={observed:?}; listener_cleanup={released:?}"
    );
    assert!(
        matches!(released, Ok(Ok(()))),
        "dropped server listener did not settle: {released:?}"
    );
}

#[tokio::test]
async fn scripted_http_finish_joins_and_preserves_recorded_request() {
    let mut server =
        spawn_scripted_http_server(vec![ScriptedResponse::json("{\"ok\":true}")]).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .retry(reqwest::retry::never())
        .build()
        .unwrap();
    let observed = async {
        client
            .post(server.base_url().join("settled").unwrap())
            .body("owned-request")
            .send()
            .await?
            .text()
            .await
    }
    .await;
    let settled = server.finish(Duration::from_secs(1)).await;
    let replay = server.finish(Duration::from_secs(1)).await;
    assert!(
        matches!(&observed, Ok(body) if body == "{\"ok\":true}"),
        "response={observed:?}; settlement={settled:?}; replay={replay:?}"
    );
    assert_eq!(settled, Ok(()));
    assert_eq!(replay, settled);
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/settled");
    assert_eq!(requests[0].body, b"owned-request");
}

#[tokio::test]
async fn scripted_http_finish_retains_task_panic() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let mut server = spawn_scripted_http_server(vec![ScriptedResponse::json("{}")]).await;
    let address = server.base_url().socket_addrs(|| None).unwrap()[0];
    let delivered = tokio::time::timeout(Duration::from_secs(2), async {
        let mut stream = TcpStream::connect(address).await?;
        stream
            .write_all(
                b"POST /malformed HTTP/1.1\r\nHost: localhost\r\nContent-Length: invalid\r\n\r\n",
            )
            .await?;
        stream.shutdown().await
    })
    .await;
    let settled = server.finish(Duration::from_secs(1)).await;
    let replay = server.finish(Duration::from_secs(1)).await;
    assert!(
        matches!(delivered, Ok(Ok(()))),
        "request={delivered:?}; settlement={settled:?}; replay={replay:?}"
    );
    assert!(
        settled
            .as_ref()
            .is_err_and(|error| error.contains("panicked")),
        "server panic must remain visible: {settled:?}"
    );
    assert_eq!(replay, settled);
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn scripted_http_finish_timeout_cancels_and_joins_pending_accept() {
    use tokio::net::TcpStream;

    let mut server = spawn_scripted_http_server(vec![ScriptedResponse::json("{}")]).await;
    let address = server.base_url().socket_addrs(|| None).unwrap()[0];
    let settled = server.finish(Duration::from_millis(10)).await;
    let replay = server.finish(Duration::from_secs(1)).await;
    let connection =
        tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(address)).await;
    assert!(
        settled
            .as_ref()
            .is_err_and(|error| error.contains("timed out")),
        "deadline must remain a failure: settlement={settled:?}; replay={replay:?}; connection={connection:?}"
    );
    assert_eq!(replay, settled);
    assert!(
        matches!(connection, Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused),
        "settled cancellation must release its listener"
    );
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn scripted_http_cancelled_finish_observer_retains_the_task() {
    let mut server =
        spawn_scripted_http_server(vec![ScriptedResponse::json("{\"still_owned\":true}")]).await;
    let lost_observer = tokio::time::timeout(
        Duration::from_millis(10),
        server.finish(Duration::from_secs(2)),
    )
    .await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .retry(reqwest::retry::never())
        .build()
        .unwrap();
    let observed = async { client.get(server.base_url()).send().await?.text().await }.await;
    let settled = server.finish(Duration::from_secs(1)).await;
    assert!(
        lost_observer.is_err(),
        "observer should end before unrequested work: observer={lost_observer:?}; response={observed:?}; settlement={settled:?}"
    );
    assert!(
        matches!(&observed, Ok(body) if body == "{\"still_owned\":true}"),
        "dropping an observation must retain the server: response={observed:?}; settlement={settled:?}"
    );
    assert_eq!(settled, Ok(()));
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn diagnostic_resolution_retry_backoff_preserves_the_current_rejection() {
    let body = json!({"error": {"type": "insufficient_quota", "code": "insufficient_quota", "message": "private-retry-evidence"}}).to_string();
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::error(429, &body)
            .with_header("retry-after", "30")
            .with_header("x-request-id", "private-retry-request"),
    ])
    .await;
    let transport = TransportConfig::new(
        Duration::from_secs(2),
        Duration::from_secs(1),
        Duration::from_secs(1),
        providers_kit::config::RetryPolicy::default(),
    )
    .unwrap();
    let provider = Provider::from_config(ProviderConfig::new(
        ProviderKind::OpenAi(
            OpenAiConfig::new(ApiKey::new("fixture-key").unwrap()).with_base_url(server.base_url()),
        ),
        transport,
    ))
    .unwrap();
    let result = provider.list_models(ModelListRequest::default()).await;
    let shutdown = provider.shutdown().await;
    let error = result.expect_err("quota rejection is not successful discovery");
    shutdown.unwrap();
    assert_eq!(error.kind(), ErrorKind::QuotaExceeded);
    assert_eq!(error.http_status(), Some(429));
    assert_eq!(error.context().operation, Operation::ListModels);
    assert_eq!(error.context().phase, ErrorPhase::ResponseHeaders);
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("private-retry-request")
    );
    assert_eq!(error.native_details().body.as_deref(), Some(body.as_str()));
    assert_eq!(server.requests().len(), 1);
    assert!(!error.to_string().contains("private-"));
}

#[tokio::test]
async fn diagnostic_resolution_retry_success_and_generation_fail_fast_keep_policy() {
    let rejection = json!({"error": {"code": "rate_limit_exceeded", "type": "rate_limit_error", "message": "private retry control"}}).to_string();
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::error(429, &rejection).with_header("retry-after", "0"),
        ScriptedResponse::json("{\"data\":[]}"),
    ])
    .await;
    let provider = WireProvider::OpenAi.build(server.base_url());
    let models = provider.list_models(ModelListRequest::default()).await;
    provider.shutdown().await.unwrap();
    assert!(models.unwrap().models.is_empty());
    assert_eq!(
        server.requests().len(),
        2,
        "retryable discovery replays exactly once"
    );

    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::error(429, &rejection).with_header("retry-after", "0"),
    ])
    .await;
    let provider = WireProvider::OpenAi.build(server.base_url());
    let model = WireProvider::OpenAi.model(&provider, "wire-model");
    let error = model
        .generate(&WireRequest::user("probe"))
        .await
        .unwrap_err();
    provider.shutdown().await.unwrap();
    assert_eq!(error.kind(), ErrorKind::RateLimited);
    assert_eq!(server.requests().len(), 1, "generation remains fail-fast");
}

#[tokio::test]
async fn diagnostic_resolution_unary_decode_keeps_structured_observations() {
    for (kind, model, body) in [
        (
            WireProvider::OpenAi,
            "wire-model",
            json!({"output": "wrong-shape"}),
        ),
        (
            WireProvider::Anthropic,
            "wire-model",
            json!({"content": "wrong-shape"}),
        ),
        (
            WireProvider::Gemini,
            "models/gemini-wire",
            json!({"candidates": "wrong-shape"}),
        ),
        (
            WireProvider::Ollama,
            "wire-model",
            json!({"message": "wrong-shape"}),
        ),
    ] {
        let server = spawn_scripted_http_server(vec![
            ScriptedResponse::json(&body.to_string())
                .with_header("x-request-id", "private-decode-request"),
        ])
        .await;
        let mut base_url = server.base_url();
        if matches!(kind, WireProvider::Ollama) {
            base_url.set_path("/api");
        }
        let provider = kind.build(base_url);
        let model = kind.model(&provider, model);
        let result = model.generate(&WireRequest::user("probe")).await;
        let shutdown = provider.shutdown().await;
        let error = result.expect_err("wrong response shape must fail");
        shutdown.unwrap();
        assert_eq!(error.context().operation, Operation::Generate);
        assert_eq!(error.context().phase, ErrorPhase::Decode);
        assert!(matches!(
            error.response_details(),
            Some(ResponseDetails::Json {
                category: JsonErrorCategory::Data,
                ..
            })
        ));
        assert_eq!(
            error.native_details().request_id.as_deref(),
            Some("private-decode-request")
        );
    }
}

#[tokio::test]
async fn diagnostic_resolution_file_delete_decode_keeps_parser_observations() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json("{")]).await;
    let provider = WireProvider::Anthropic.build(server.base_url());
    let error = provider
        .delete_file("file_fixture".into())
        .await
        .expect_err("malformed delete reply fails");
    provider.shutdown().await.unwrap();
    assert_eq!(error.context().operation, Operation::DeleteFile);
    assert_eq!(error.context().phase, ErrorPhase::Decode);
    assert_eq!(
        error.response_details(),
        Some(&ResponseDetails::Json {
            category: JsonErrorCategory::Eof,
            line: 1,
            column: 1
        })
    );
}

#[tokio::test]
async fn diagnostic_resolution_ollama_terminal_reason_precedes_calls() {
    for native_reason in ["stop", "length", "error"] {
        for has_calls in [false, true] {
            for stream_layout in [0, 1, 2] {
                let calls = if has_calls {
                    vec![json!({"function": {"name": "lookup", "arguments": {"key": "fixture"}}})]
                } else {
                    Vec::new()
                };
                let message = json!({"role": "assistant", "content": "partial", "thinking": "private-thinking", "tool_calls": calls});
                let mut terminal = json!({"model": "wire-model", "done": true, "done_reason": native_reason,
                    "message": message, "prompt_eval_count": 3, "eval_count": 2});
                let reply = match stream_layout {
                    0 => ScriptedResponse::json(&terminal.to_string()),
                    1 => ScriptedResponse::chunks(
                        200,
                        "application/x-ndjson",
                        vec![format!("{terminal}\n").into_bytes()],
                    ),
                    _ => {
                        terminal["message"] = json!({"role": "assistant", "content": ""});
                        let earlier =
                            json!({"model": "wire-model", "done": false, "message": message});
                        ScriptedResponse::chunks(
                            200,
                            "application/x-ndjson",
                            vec![format!("{earlier}\n{terminal}\n").into_bytes()],
                        )
                    }
                };
                let server = spawn_scripted_http_server(vec![reply]).await;
                let mut base_url = server.base_url();
                base_url.set_path("/api");
                let provider = WireProvider::Ollama.build(base_url);
                let model = WireProvider::Ollama.model(&provider, "wire-model");
                let request = WireRequest::user("probe");
                let response = if stream_layout != 0 {
                    let events = model
                        .generate_stream(&request)
                        .await
                        .unwrap()
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();
                    let completed = events
                        .into_iter()
                        .filter_map(|event| match event {
                            GenerateEvent::Completed(response) => Some(response),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(completed.len(), 1);
                    *completed.into_iter().next().unwrap()
                } else {
                    model.generate(&request).await.unwrap()
                };
                provider.shutdown().await.unwrap();
                let expected = match native_reason {
                    "stop" if has_calls => FinishReason::ToolCall,
                    "stop" => FinishReason::Stop,
                    "length" => FinishReason::Length,
                    _ => FinishReason::Error,
                };
                assert_eq!(
                    response.finish_reason,
                    Some(expected),
                    "{native_reason}, has_calls={has_calls}, layout={stream_layout}"
                );
                assert_eq!(
                    response.message.function_calls().len(),
                    usize::from(has_calls)
                );
                if has_calls {
                    assert_eq!(
                        response.message.function_calls()[0].arguments(),
                        &json!({"key": "fixture"})
                    );
                }
                assert!(
                    response.message.content().iter().any(
                        |part| matches!(part, ContentPart::Text { text } if text == "partial")
                    )
                );
                assert!(response.message.continuation().is_some());
                assert_eq!(response.usage.input_tokens, Some(3));
                assert_eq!(response.usage.output_tokens, Some(2));
            }
        }
    }
}

#[tokio::test]
async fn diagnostic_resolution_ollama_invalid_terminal_never_completes() {
    for invalid in ["missing", "unknown", "malformed_call"] {
        for streamed in [false, true] {
            let call = json!({"function": {"name": "lookup", "arguments": {"key": "fixture"}}});
            let mut terminal = json!({"model": "wire-model", "done": true, "done_reason": "length", "message": {"role": "assistant", "content": "partial", "tool_calls": [call]}});
            match invalid {
                "missing" => {
                    terminal.as_object_mut().unwrap().remove("done_reason");
                }
                "unknown" => terminal["done_reason"] = json!("private-unknown-reason"),
                _ => {
                    terminal["message"]["tool_calls"][0]["function"]["arguments"] =
                        json!("invalid-argument-shape")
                }
            }
            let reply = if streamed {
                ScriptedResponse::chunks(
                    200,
                    "application/x-ndjson",
                    vec![format!("{terminal}\n").into_bytes()],
                )
            } else {
                ScriptedResponse::json(&terminal.to_string())
            };
            let server = spawn_scripted_http_server(vec![reply]).await;
            let mut base_url = server.base_url();
            base_url.set_path("/api");
            let provider = WireProvider::Ollama.build(base_url);
            let model = WireProvider::Ollama.model(&provider, "wire-model");
            let request = WireRequest::user("probe");
            let error = if streamed {
                let events = model
                    .generate_stream(&request)
                    .await
                    .unwrap()
                    .collect::<Vec<_>>()
                    .await;
                assert!(
                    !events
                        .iter()
                        .any(|event| matches!(event, Ok(GenerateEvent::Completed(_))))
                );
                events
                    .into_iter()
                    .find_map(Result::err)
                    .expect("invalid terminal emits an error")
            } else {
                model
                    .generate(&request)
                    .await
                    .expect_err("invalid terminal fails")
            };
            provider.shutdown().await.unwrap();
            assert!(matches!(error.kind(), ErrorKind::InvalidResponse(_)));
            if invalid == "unknown" {
                assert_eq!(
                    error.response_details(),
                    Some(&ResponseDetails::Discriminator {
                        actual: "private-unknown-reason".into()
                    })
                );
                assert!(!format!("{error:?}").contains("private-unknown-reason"));
            }
        }
    }
    let prior = json!({"model": "wire-model", "done": false, "message": {"role": "assistant", "content": "partial", "tool_calls": [{"function": {"name": "lookup", "arguments": {}}}]}});
    let server = spawn_scripted_http_server(vec![ScriptedResponse::chunks(
        200,
        "application/x-ndjson",
        vec![format!("{prior}\n{{\"error\":\"private upstream failure\"}}\n").into_bytes()],
    )])
    .await;
    let mut base_url = server.base_url();
    base_url.set_path("/api");
    let provider = WireProvider::Ollama.build(base_url);
    let model = WireProvider::Ollama.model(&provider, "wire-model");
    let events = model
        .generate_stream(&WireRequest::user("probe"))
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    provider.shutdown().await.unwrap();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(GenerateEvent::Completed(_))))
    );
    assert!(events.iter().any(
        |event| matches!(event, Ok(GenerateEvent::ContentDelta { delta }) if delta == "partial")
    ));
    assert!(
        events.iter().any(
            |event| matches!(event, Err(error) if error.kind() == ErrorKind::ProviderRejected)
        )
    );
}

#[tokio::test]
async fn authoritative_authentication_status_survives_conflicting_missing_model_code() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::error(
        401,
        &json!({"error": {
            "code": "model_not_found",
            "type": "invalid_request_error",
            "message": "sensitive conflicting native evidence"
        }})
        .to_string(),
    )])
    .await;
    let provider = WireProvider::OpenAi.build(server.base_url());
    let model = WireProvider::OpenAi.model(&provider, "exact-missing-model");
    let error = model
        .generate(&WireRequest::user("probe"))
        .await
        .unwrap_err();
    provider.shutdown().await.unwrap();
    assert_eq!(server.requests().len(), 1);
    assert_eq!(error.http_status(), Some(401));
    assert_eq!(
        error.native_details().code.as_deref(),
        Some("model_not_found")
    );
    assert_eq!(error.kind(), ErrorKind::Authentication);
}

#[tokio::test]
async fn gemini_http_canonical_precondition_classification() {
    for (http_status, native_status, expected_kind, expected_basis) in [
        (
            400,
            "FAILED_PRECONDITION",
            ErrorKind::FailedPrecondition,
            ClassificationBasis::CanonicalStatus,
        ),
        (
            400,
            "INVALID_ARGUMENT",
            ErrorKind::InvalidRequest,
            ClassificationBasis::CanonicalStatus,
        ),
        (
            400,
            "NOT_FOUND",
            ErrorKind::InvalidRequest,
            ClassificationBasis::HttpStatus { status: 400 },
        ),
        (
            422,
            "FAILED_PRECONDITION",
            ErrorKind::InvalidRequest,
            ClassificationBasis::HttpStatus { status: 422 },
        ),
        (
            401,
            "FAILED_PRECONDITION",
            ErrorKind::Authentication,
            ClassificationBasis::HttpStatus { status: 401 },
        ),
        (
            403,
            "FAILED_PRECONDITION",
            ErrorKind::PermissionDenied,
            ClassificationBasis::HttpStatus { status: 403 },
        ),
    ] {
        for streaming in [false, true] {
            let body = json!({"error": {
                "code": http_status,
                "status": native_status,
                "message": "fixture-native-precondition-detail"
            }})
            .to_string();
            let server = spawn_scripted_http_server(vec![
                ScriptedResponse::error(http_status, &body)
                    .with_header("x-request-id", "fixture-precondition-request-id"),
            ])
            .await;
            let provider = WireProvider::Gemini.build(server.base_url());
            let model = WireProvider::Gemini.model(&provider, "models/gemini-wire");
            let request = WireRequest::user("probe");
            let error = if streaming {
                model.generate_stream(&request).await.err()
            } else {
                model.generate(&request).await.err()
            };
            let shutdown = provider.shutdown().await;
            let error =
                error.expect("HTTP rejection must fail before returning a response or stream");
            shutdown.expect("provider closes after HTTP rejection");

            assert_eq!(
                error.kind(),
                expected_kind,
                "HTTP {http_status}, native {native_status}, streaming={streaming}"
            );
            assert_eq!(error.classification_basis(), expected_basis);
            assert_eq!(error.http_status(), Some(http_status));
            assert_eq!(
                error.context().operation,
                if streaming {
                    Operation::GenerateStream
                } else {
                    Operation::Generate
                }
            );
            assert_eq!(error.context().phase, ErrorPhase::ResponseHeaders);
            let remote = error
                .remote_failure()
                .expect("remote rejection is retained");
            assert_eq!(remote.kind(), expected_kind);
            assert_eq!(remote.classification_basis(), expected_basis);
            assert_eq!(remote.native_details(), error.native_details());
            let details = error.native_details();
            assert_eq!(details.native_status.as_deref(), Some(native_status));
            assert_eq!(details.reported_http_status, Some(http_status));
            assert_eq!(details.rpc_code, None);
            assert_eq!(
                details.message.as_deref(),
                Some("fixture-native-precondition-detail")
            );
            assert_eq!(details.body.as_deref(), Some(body.as_str()));
            assert_eq!(
                details.request_id.as_deref(),
                Some("fixture-precondition-request-id")
            );
            for rendered in [error.to_string(), format!("{error:?}")] {
                assert!(!rendered.contains("fixture-native-precondition-detail"));
                assert!(!rendered.contains("fixture-precondition-request-id"));
            }
            let requests = server.requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].method, "POST");
            assert_eq!(
                requests[0].path,
                if streaming {
                    "/v1beta/models/gemini-wire:streamGenerateContent?alt=sse"
                } else {
                    "/v1beta/models/gemini-wire:generateContent"
                }
            );
        }
    }
}

#[derive(Clone, Copy)]
enum WireProvider {
    OpenAi,
    Anthropic,
    Gemini,
    Ollama,
}

impl WireProvider {
    async fn observe_batch_results(
        self,
        replies: Vec<ScriptedResponse>,
        job: &str,
        limit: usize,
    ) -> Result<
        (
            Result<Vec<BatchResult<GenerationContinuation>>, KitError>,
            Vec<RecordedRequest>,
        ),
        String,
    > {
        let server = spawn_scripted_http_server(replies).await;
        let provider = self.build(server.base_url());
        let observed = tokio::time::timeout(
            Duration::from_secs(10),
            provider.batch_results(job.into(), NonZeroUsize::new(limit).unwrap()),
        )
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        eprintln!("BATCH_CLEANUP operation={observed:?}; shutdown={shutdown:?}");
        if !matches!(shutdown, Ok(Ok(()))) {
            return Err(format!("operation={observed:?}; shutdown={shutdown:?}"));
        }
        observed
            .map(|result| (result, server.requests()))
            .map_err(|error| format!("operation timeout={error}; shutdown={shutdown:?}"))
    }

    async fn stream_failure(self, reply: ScriptedResponse) -> providers_kit::KitError {
        let server = spawn_scripted_http_server(vec![reply]).await;
        let mut base_url = server.base_url();
        if matches!(self, Self::Ollama) {
            base_url.set_path("/api");
        }
        let provider = self.build(base_url);
        let selected = if matches!(self, Self::Gemini) {
            "models/gemini-wire"
        } else {
            "wire-model"
        };
        let model = self.model(&provider, selected);
        let mut stream = model
            .generate_stream(&WireRequest::user("probe"))
            .await
            .unwrap();
        let error = loop {
            match stream.next().await {
                Some(Err(error)) => break error,
                Some(Ok(GenerateEvent::Completed(_))) => {
                    panic!("invalid stream completed successfully")
                }
                Some(Ok(_)) => {}
                None => panic!("invalid stream omitted error"),
            }
        };
        assert!(stream.next().await.is_none());
        provider.shutdown().await.unwrap();
        error
    }

    async fn assert_invalid_stream_frame(
        self,
        response: ScriptedResponse,
        expected: ResponseViolation,
        phase: ErrorPhase,
    ) {
        let server = spawn_scripted_http_server(vec![
            response.with_header("x-request-id", "sensitive-field-request"),
        ])
        .await;
        let mut base_url = server.base_url();
        if matches!(self, Self::Ollama) {
            base_url.set_path("/api");
        }
        let provider = self.build(base_url);
        let model = self.model(&provider, "wire-model");
        let mut stream = model
            .generate_stream(&WireRequest::user("fixture"))
            .await
            .expect("HTTP stream should open before the invalid frame");
        let error = loop {
            match stream.next().await {
                Some(Ok(GenerateEvent::Completed(_))) => {
                    panic!("invalid frame must not produce successful completion")
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => break error,
                None => panic!("invalid frame must produce an error"),
            }
        };
        assert_eq!(error.kind(), ErrorKind::InvalidResponse(expected));
        assert_eq!(error.context().operation, Operation::GenerateStream);
        assert_eq!(error.context().phase, phase);
        assert_eq!(error.http_status(), None);
        assert_eq!(
            error.native_details().request_id.as_deref(),
            Some("sensitive-field-request")
        );
        assert_eq!(
            error.native_details().event_id.as_deref(),
            if matches!(self, Self::Ollama) {
                None
            } else {
                Some("sensitive-field-event")
            }
        );
        assert!(
            error
                .native_details()
                .body
                .as_deref()
                .is_some_and(|body| body.contains("sensitive-field-body"))
        );
        assert!(!error.to_string().contains("sensitive-"));
        assert!(!format!("{error:?}").contains("sensitive-"));
        assert!(stream.next().await.is_none(), "error must be terminal");
        assert_eq!(server.requests().len(), 1);
        provider.shutdown().await.expect("provider shutdown");
    }

    fn build(self, base_url: reqwest::Url) -> Provider {
        let kind = match self {
            Self::OpenAi => ProviderKind::OpenAi(
                OpenAiConfig::new(ApiKey::new("openai-wire-key").expect("key should validate"))
                    .with_base_url(base_url),
            ),
            Self::Anthropic => ProviderKind::Anthropic(
                AnthropicConfig::new(
                    ApiKey::new("anthropic-wire-key").expect("key should validate"),
                )
                .with_base_url(base_url),
            ),
            Self::Gemini => ProviderKind::Gemini(
                GeminiConfig::new(ApiKey::new("gemini-wire-key").expect("key should validate"))
                    .with_base_url(base_url),
            ),
            Self::Ollama => ProviderKind::Ollama(
                OllamaConfig::local()
                    .with_base_url(base_url)
                    .expect("fixture Ollama base URL should validate"),
            ),
        };
        Provider::from_config(ProviderConfig::new(kind, TransportConfig::default()))
            .expect("provider should construct")
    }

    fn model(self, provider: &Provider, id: &str) -> Model {
        provider
            .model(ModelId::new(id).expect("model id should validate"))
            .expect("exact model should construct")
    }

    fn model_at(self, base_url: reqwest::Url, id: &str) -> Model {
        self.model(&self.build(base_url), id)
    }

    fn api_model(self, mut base_url: reqwest::Url, id: &str) -> Model {
        base_url.set_path(match self {
            Self::OpenAi => "/v1",
            Self::Ollama => "/api",
            Self::Anthropic | Self::Gemini => unreachable!("fixture API root is already exact"),
        });
        self.model_at(base_url, id)
    }
}

struct WireRequest;

impl WireRequest {
    fn messages(messages: Vec<Message>) -> GenerateRequest {
        GenerateRequest::new(messages).expect("request should validate")
    }

    fn user(text: &str) -> GenerateRequest {
        Self::messages(vec![
            Message::user_text(text).expect("message should validate"),
        ])
    }

    fn continuation(message: Message, text: &str) -> GenerateRequest {
        Self::messages(vec![
            message,
            Message::user_text(text).expect("message should validate"),
        ])
    }

    fn function_output(
        message: Message,
        call: &FunctionCall,
        content: Vec<ContentPart>,
    ) -> GenerateRequest {
        let result = FunctionResult::new(
            call.id().map(str::to_owned),
            call.name().to_owned(),
            content,
            false,
        )
        .expect("function result should validate");
        Self::messages(vec![
            message,
            Message::tool(vec![result]).expect("function result message should validate"),
        ])
    }
}

struct WireResponse;

impl WireResponse {
    fn openai_batch_success_row() -> String {
        format!(
            "{}\n",
            json!({"custom_id":"sensitive-good","response":{"status_code":200,
                "request_id":"sensitive-row-request","body":{"model":"exact-native-model",
                "status":"completed","output":[{"type":"message","role":"assistant",
                "content":[{"type":"output_text","text":"fixture answer"}]}]}},"error":null})
        )
    }

    fn openai_batch_error_row(id: &str) -> String {
        format!(
            "{}\n",
            json!({"custom_id":id,"response":{"status_code":400,
                "request_id":"sensitive-error-row-request","body":{"error":{
                "type":"invalid_request_error","code":"invalid_file",
                "message":"sensitive row detail"}}},"error":null})
        )
    }

    fn openai_batch_files(second: ScriptedResponse) -> Vec<ScriptedResponse> {
        vec![
            ScriptedResponse::json(r#"{"id":"batch_remote","status":"completed","output_file_id":"file-one","error_file_id":"file-two"}"#)
                .with_header("x-request-id", "sensitive-job-request"),
            ScriptedResponse::json(&Self::openai_batch_success_row())
                .with_header("x-request-id", "sensitive-output-request"),
            second.with_header("x-request-id", "sensitive-error-file-request"),
        ]
    }

    fn gemini_batch_success_row() -> Value {
        json!({"key":"sensitive-good","response":{"modelVersion":"gemini-native-version",
            "candidates":[{"content":{"role":"model","parts":[{"text":"fixture answer"}]},
            "finishReason":"STOP"}]}})
    }

    fn gemini_batch_files(second: &str) -> Vec<ScriptedResponse> {
        vec![
            ScriptedResponse::json(r#"{"name":"batches/wire","metadata":{"state":"JOB_STATE_SUCCEEDED"},"response":{"responsesFile":"files/output"}}"#)
                .with_header("x-request-id", "sensitive-job-request"),
            ScriptedResponse::json(&format!("\n{}\n \t\n{second}\n", Self::gemini_batch_success_row()))
                .with_header("x-request-id", "sensitive-download-request"),
        ]
    }

    fn anthropic_text(id: &str, model: &str, text: &str) -> ScriptedResponse {
        let response = json!({
            "id": id,
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": text}],
            "model": model,
            "stop_reason": "end_turn"
        });
        ScriptedResponse::json(&response.to_string())
    }
}

#[test]
fn message_deserialization_rejects_invalid_role_shapes_and_unverifiable_thinking() {
    let invalid = [
        json!({
            "role": "system",
            "content": [{"type": "image", "media_type": "image/png", "data": "AQ=="}]
        }),
        json!({
            "role": "user",
            "content": [{"type": "json", "value": {"not": "ordinary content"}}]
        }),
        json!({
            "role": "tool",
            "content": [{"type": "text", "text": "not a function result"}]
        }),
        json!({
            "role": "assistant",
            "content": [{"type": "thinking", "text": "unsigned replay"}]
        }),
    ];

    for value in invalid {
        assert!(
            serde_json::from_value::<Message<OpaqueContinuation>>(value).is_err(),
            "invalid role-specific message shape should fail deserialization"
        );
    }
}

#[test]
fn message_serde_redacts_opaque_continuation_and_base64_encodes_media() {
    let continued = Message::<OpaqueContinuation>::assistant_text("visible")
        .expect("assistant message should validate")
        .with_continuation(OpaqueContinuation)
        .expect("assistant continuation should validate");
    let error = serde_json::to_value(&continued)
        .expect_err("opaque continuation must fail serialization without a serde bound");
    assert!(error.to_string().contains("provider continuation"));

    let media = Message::<OpaqueContinuation>::user(vec![
        ContentPart::Image {
            media_type: "image/png".to_string(),
            data: vec![0, 1, 2],
        },
        ContentPart::Audio {
            media_type: "audio/pcm".to_string(),
            data: vec![3, 4, 5],
        },
        ContentPart::Document {
            media_type: "application/pdf".to_string(),
            data: vec![6, 7, 8],
            file_name: Some("fixture.pdf".to_string()),
        },
    ])
    .expect("user media message should validate");
    let encoded = serde_json::to_value(media).expect("media message should serialize");
    assert_eq!(encoded["content"][0]["data"], "AAEC");
    assert_eq!(encoded["content"][1]["data"], "AwQF");
    assert_eq!(encoded["content"][2]["data"], "BgcI");
}

#[tokio::test]
async fn response_diagnostics_are_bounded_and_transport_urls_are_redacted() {
    let mut oversized_body = "x".repeat(64 * 1024);
    oversized_body.push_str("trailing-secret");
    let mut oversized_semantic = "y".repeat(64 * 1024);
    oversized_semantic.push_str("semantic-trailing-secret");
    let malformed_file = json!({
        "id": "file-semantic",
        "status": "unknown",
        "metadata": {"oversized": oversized_semantic},
    })
    .to_string();
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::error(400, &oversized_body),
        ScriptedResponse::json(&malformed_file),
    ])
    .await;
    let provider = WireProvider::OpenAi.build(server.base_url());
    let error = provider
        .list_files(FileListRequest::default())
        .await
        .expect_err("malformed oversized provider failure should remain bounded");
    assert_eq!(error.http_status(), Some(400));
    let body = error
        .native_details()
        .body
        .as_ref()
        .expect("non-success response should return a structured provider error");
    assert!(body.ends_with("[response body truncated]"));
    assert!(!body.contains("trailing-secret"));
    assert!(body.len() < 65 * 1024);

    let error = provider
        .file_info("file-semantic".to_string())
        .await
        .expect_err("malformed semantic response diagnostics should remain bounded");
    assert!(matches!(error.kind(), ErrorKind::InvalidResponse(_)));
    let body = error
        .native_details()
        .body
        .as_ref()
        .expect("malformed semantic response should retain its bounded native diagnostic");
    assert!(body.ends_with("[response body truncated]"));
    assert!(!body.contains("semantic-trailing-secret"));
    assert!(body.len() < 65 * 1024);

    let mut results_url = server.base_url();
    results_url
        .set_username("diagnostic-user")
        .expect("URL username should set");
    results_url
        .set_password(Some("diagnostic-password"))
        .expect("URL password should set");
    results_url.set_query(Some("api_key=diagnostic-query"));
    let batch = json!({
        "id": "msgbatch_diagnostic",
        "processing_status": "ended",
        "results_url": results_url,
    })
    .to_string();
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json(&batch)]).await;
    let provider = WireProvider::Anthropic.build(server.base_url());
    let error = provider
        .batch_results(
            "msgbatch_diagnostic".to_string(),
            NonZeroUsize::new(1024).expect("result limit should be nonzero"),
        )
        .await
        .expect_err("provider-returned URLs with userinfo must be rejected");
    let diagnostic = format!("{error:?}");
    assert!(!diagnostic.contains("diagnostic-user"));
    assert!(!diagnostic.contains("diagnostic-password"));
    assert!(!diagnostic.contains("diagnostic-query"));
}

#[tokio::test]
async fn bounded_http_bodies_cover_exact_chunked_overflow_and_lossy_diagnostics() {
    let exact = vec![b'a'; 128];
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::chunks(
            200,
            "application/octet-stream",
            vec![exact[..64].to_vec(), exact[64..].to_vec()],
        ),
        ScriptedResponse::chunks(
            200,
            "application/octet-stream",
            vec![vec![b'b'; 64], vec![b'b'; 65]],
        ),
        ScriptedResponse::chunks(
            400,
            "application/json",
            vec![vec![0xff; 32 * 1024], vec![0xff; 32 * 1024]],
        ),
    ])
    .await;
    let provider = WireProvider::OpenAi.build(server.base_url());
    let limit = NonZeroUsize::new(128).expect("limit must be non-zero");

    assert_eq!(
        provider
            .download_file("file-exact".to_string(), limit)
            .await
            .expect("an exact-limit chunked body should succeed"),
        exact
    );
    assert!(matches!(
        provider
            .download_file("file-overflow".to_string(), limit)
            .await,
        Err(ref error) if error.kind() == ErrorKind::InvalidResponse(ResponseViolation::LimitExceeded { limit: 128 }) && error.native_details().body.is_none()
    ));

    let error = provider
        .list_files(FileListRequest::default())
        .await
        .expect_err("invalid provider bytes should remain a bounded diagnostic");
    assert_eq!(error.http_status(), Some(400));
    let body = error
        .native_details()
        .body
        .as_ref()
        .expect("non-success response should retain a bounded provider body");
    assert!(body.len() <= 64 * 1024);
    assert!(body.ends_with("[response body truncated]"));
}

#[tokio::test]
async fn anthropic_cross_origin_batch_results_do_not_receive_credentials() {
    let results = spawn_scripted_http_server(vec![ScriptedResponse::json("")]).await;
    let mut results_url = results.base_url();
    results_url.set_query(Some("signed=fixture"));
    let batch = json!({
        "id": "msgbatch_cross_origin",
        "processing_status": "ended",
        "results_url": results_url,
    })
    .to_string();
    let api = spawn_scripted_http_server(vec![ScriptedResponse::json(&batch)]).await;
    let provider = WireProvider::Anthropic.build(api.base_url());

    let rows = provider
        .batch_results(
            "msgbatch_cross_origin".to_string(),
            NonZeroUsize::new(1024).expect("result limit should be nonzero"),
        )
        .await
        .expect("signed cross-origin results should be readable without provider credentials");
    assert!(rows.is_empty());
    let requests = results.requests();
    assert_eq!(requests[0].path, "/?signed=fixture");
    assert!(!requests[0].headers.contains_key("x-api-key"));
}

#[tokio::test]
async fn openai_batch_success_accepts_the_documented_null_error_field() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(
            r#"{
                "id": "batch_wire",
                "status": "completed",
                "output_file_id": "file-output"
            }"#,
        ),
        ScriptedResponse::json(
            r#"{"id":"batch_req_1","custom_id":"row-success","response":{"status_code":200,"request_id":"req_1","body":{"model":"gpt-wire-20260826","status":"completed","output":[{"id":"msg_batch","type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}},"error":null}"#,
        ),
    ])
    .await;
    let mut base_url = server.base_url();
    base_url.set_path("/v1");
    let provider = WireProvider::OpenAi.build(base_url);

    let results = provider
        .batch_results(
            "batch_wire".to_string(),
            NonZeroUsize::new(4_096).expect("result limit should be nonzero"),
        )
        .await
        .expect("documented successful batch row should normalize");
    assert!(matches!(
        results.as_slice(),
        [BatchResult::Succeeded { custom_id, output }]
            if custom_id == "row-success" && output.model.as_str() == "gpt-wire-20260826"
    ));
}

#[tokio::test]
async fn provider_file_batches_render_for_gemini_and_reject_unsupported_providers() {
    let gemini_server = spawn_scripted_http_server(vec![ScriptedResponse::json(
        r#"{"name":"batches/provider-file","metadata":{"state":"JOB_STATE_PENDING"}}"#,
    )])
    .await;
    let gemini =
        WireProvider::Gemini.model_at(gemini_server.base_url(), "models/gemini-3-flash-preview");
    let submitted = gemini
        .submit_batch(&BatchSubmitRequest::ProviderFile {
            file_id: "files/provider-batch".to_string(),
        })
        .await
        .expect("Gemini provider-file batch should submit without rewriting the resource");
    assert_eq!(submitted.id, "batches/provider-file");
    assert_eq!(submitted.status, BatchStatus::Queued);
    let requests = gemini_server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].path,
        "/v1beta/models/gemini-3-flash-preview:batchGenerateContent"
    );
    assert_eq!(
        requests[0]
            .headers
            .get("x-goog-api-key")
            .map(String::as_str),
        Some("gemini-wire-key")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[0].body)
            .expect("Gemini batch body should be JSON")["batch"]["inputConfig"]["fileName"],
        "files/provider-batch"
    );

    let unsupported_server = spawn_scripted_http_server(Vec::new()).await;
    let request = BatchSubmitRequest::ProviderFile {
        file_id: "opaque-provider-file".to_string(),
    };
    let anthropic =
        WireProvider::Anthropic.model_at(unsupported_server.base_url(), "claude-sonnet-4-6");
    assert!(matches!(
        anthropic.submit_batch(&request).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    let ollama = WireProvider::Ollama.api_model(unsupported_server.base_url(), "llama3");
    assert!(matches!(
        ollama.submit_batch(&request).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    assert!(unsupported_server.requests().is_empty());
}

#[tokio::test]
async fn malformed_stream_diagnostics_are_bounded_for_every_sse_provider() {
    let mut malformed_payload = "{".to_string();
    malformed_payload.push_str(&"x".repeat(70 * 1024));
    let stream_body = format!("data: {malformed_payload}\n\n");
    let request = WireRequest::user("stream")
        .with_generation(
            GenerationConfig::default()
                .with_max_output_tokens(NonZeroU32::new(64).expect("token limit must be non-zero")),
        )
        .expect("generation config should validate");

    for (provider_kind, provider_name, model_id) in [
        (WireProvider::OpenAi, "OpenAI", "gpt-4.1"),
        (WireProvider::Anthropic, "Anthropic", "claude-wire"),
        (WireProvider::Gemini, "Gemini", "models/gemini-wire"),
    ] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::sse(&stream_body)]).await;
        let model = provider_kind.model_at(server.base_url(), model_id);
        let mut stream = model
            .generate_stream(&request)
            .await
            .expect("stream should open before its malformed frame is decoded");
        let error = stream
            .next()
            .await
            .expect("malformed stream should yield one error")
            .expect_err("malformed stream frame must not normalize");
        assert!(
            matches!(error.kind(), ErrorKind::InvalidResponse(_)),
            "malformed {provider_name} stream should retain a bounded diagnostic body"
        );
        let message = error
            .native_details()
            .message
            .as_ref()
            .expect("decode message retained");
        let body = error
            .native_details()
            .body
            .as_ref()
            .expect("decode body retained");
        assert!(!message.contains("payload="));
        assert!(message.len() < 1024);
        assert!(body.ends_with("[response body truncated]"));
        assert!(body.len() < 65 * 1024);
    }
}

#[tokio::test]
async fn gemini_inline_batch_results_honor_the_caller_body_cap() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json(&format!(
        r#"{{"name":"batches/wire","response":{{"inlinedResponses":{{"inlinedResponses":[{{"metadata":{{"custom_id":"row"}},"response":{{"modelVersion":"models/gemini-wire","candidates":[{{"content":{{"role":"model","parts":[{{"text":"{}"}}]}}}}]}}}}]}}}}}}"#,
        "x".repeat(512)
    ))])
    .await;
    let provider = WireProvider::Gemini.build(server.base_url());
    let error = provider
        .batch_results(
            "batches/wire".to_string(),
            NonZeroUsize::new(128).expect("result limit must be non-zero"),
        )
        .await
        .expect_err("inline operation response must honor the caller result limit");
    assert!(matches!(
        error,
        ref error if error.kind() == ErrorKind::InvalidResponse(ResponseViolation::LimitExceeded { limit: 128 }) && error.native_details().body.is_none()
    ));
}

#[tokio::test]
async fn gemini_missing_batch_results_follow_the_terminal_state() {
    let operation = |state: &str, error: &str| {
        format!(r#"{{"name":"batches/wire","metadata":{{"state":"{state}"}}{error}}}"#)
    };
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(&operation("JOB_STATE_RUNNING", "")),
        ScriptedResponse::json(&operation("JOB_STATE_SUCCEEDED", "")),
        ScriptedResponse::json(&operation(
            "JOB_STATE_FAILED",
            r#","error":{"code":13,"message":"native batch failure"}"#,
        )),
        ScriptedResponse::json(&operation("JOB_STATE_CANCELLED", "")),
        ScriptedResponse::json(&operation("JOB_STATE_EXPIRED", "")),
    ])
    .await;
    let provider = WireProvider::Gemini.build(server.base_url());
    let max_bytes = NonZeroUsize::new(4_096).expect("result limit must be non-zero");

    assert!(matches!(
        provider
            .batch_results("batches/wire".to_string(), max_bytes)
            .await,
        Err(ref error) if error.kind() == ErrorKind::InvalidState && error.native_details().message.as_deref().is_some_and(|message| message.contains("not ready"))
    ));
    assert!(matches!(
        provider
            .batch_results("batches/wire".to_string(), max_bytes)
            .await,
        Err(ref error) if matches!(error.kind(), ErrorKind::InvalidResponse(_)) && error.native_details().message.as_deref().is_some_and(|message| message.contains("completed"))
    ));
    assert!(matches!(
        provider
            .batch_results("batches/wire".to_string(), max_bytes)
            .await,
        Err(ref error) if error.kind() == ErrorKind::Internal
            && error.native_details().rpc_code == Some(13)
            && error.native_details().message.as_deref() == Some("native batch failure")
    ));
    assert!(matches!(
        provider
            .batch_results("batches/wire".to_string(), max_bytes)
            .await,
        Err(ref error) if error.kind() == ErrorKind::InvalidState && error.native_details().message.as_deref().is_some_and(|message| message.contains("cancelled"))
    ));
    assert!(matches!(
        provider
            .batch_results("batches/wire".to_string(), max_bytes)
            .await,
        Err(ref error) if error.kind() == ErrorKind::InvalidState && error.native_details().message.as_deref().is_some_and(|message| message.contains("expired"))
    ));
}

#[tokio::test]
async fn gemini_accepts_an_empty_protojson_files_page() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json("{}")]).await;
    let provider = WireProvider::Gemini.build(server.base_url());

    let response = provider
        .list_files(
            FileListRequest::default()
                .with_limit(NonZeroUsize::new(10).expect("limit must be non-zero")),
        )
        .await
        .expect("an omitted empty repeated field should decode as an empty files page");

    assert!(response.files.is_empty());
    assert_eq!(response.next_cursor, None);
}

#[tokio::test]
async fn gemini_keeps_file_lifecycle_ids_distinct_from_generation_uris() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(
            r#"{
                "name": "files/lifecycle-id",
                "displayName": "fixture.pdf",
                "mimeType": "application/pdf",
                "uri": "https://generativelanguage.googleapis.com/v1beta/files/generation-uri",
                "state": "ACTIVE"
            }"#,
        ),
        ScriptedResponse::json(r#"{"name":"files/different-id","state":"ACTIVE"}"#),
        ScriptedResponse::json("{}"),
        ScriptedResponse::json(r#"{"unexpected":true}"#),
    ])
    .await;
    let provider = WireProvider::Gemini.build(server.base_url());

    let file = provider
        .file_info("files/lifecycle-id".to_string())
        .await
        .expect("matching lifecycle metadata should normalize");
    assert_eq!(file.id, "files/lifecycle-id");
    let Some(FileReference::Url { url, .. }) = file.input_reference else {
        panic!("Gemini file should expose its distinct generation URI as input reference");
    };
    assert_eq!(
        url.as_str(),
        "https://generativelanguage.googleapis.com/v1beta/files/generation-uri"
    );

    assert!(matches!(
        provider.file_info("files/lifecycle-id".to_string()).await,
        Err(ref error) if matches!(error.kind(), ErrorKind::InvalidResponse(_))
    ));
    provider
        .delete_file("files/lifecycle-id".to_string())
        .await
        .expect("documented empty JSON object should acknowledge deletion");
    assert!(matches!(
        provider.delete_file("files/lifecycle-id".to_string()).await,
        Err(ref error) if matches!(error.kind(), ErrorKind::InvalidResponse(_))
    ));
    assert!(matches!(
        provider
            .download_file(
                "files/lifecycle-id".to_string(),
                NonZeroUsize::new(1024).expect("download limit must be non-zero"),
            )
            .await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    assert_eq!(server.requests().len(), 4);
}

#[tokio::test]
async fn anthropic_download_requires_explicit_downloadable_metadata() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
        r#"{"id":"file_omitted","type":"file","filename":"fixture.txt","mime_type":"text/plain","size_bytes":3}"#,
    )])
    .await;
    let provider = WireProvider::Anthropic.build(server.base_url());

    assert!(matches!(
        provider
            .download_file(
                "file_omitted".to_owned(),
                NonZeroUsize::new(1024).expect("download limit must be non-zero"),
            )
            .await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/files/file_omitted");
}

#[tokio::test]
async fn built_in_uploads_preserve_identity_mime_and_safe_resumable_urls() {
    let openai_server = spawn_scripted_http_server(vec![ScriptedResponse::json(
        r#"{"id":"file-upload","filename":"fixture.txt","bytes":3,"status":"processed","mime_type":"text/plain"}"#,
    )])
    .await;
    let mut openai_base = openai_server.base_url();
    openai_base.set_path("/v1");
    let openai = WireProvider::OpenAi.build(openai_base);
    let uploaded = openai
        .upload_file(
            FileUploadRequest::new("fixture.txt", "text/plain", b"abc".to_vec())
                .expect("OpenAI upload request should validate"),
        )
        .await
        .expect("OpenAI upload should normalize");
    assert_eq!(uploaded.id, "file-upload");
    assert_eq!(uploaded.media_type.as_deref(), Some("text/plain"));
    assert_eq!(
        uploaded.input_reference,
        Some(FileReference::Provider {
            id: "file-upload".to_string(),
            media_type: Some("text/plain".to_string()),
        })
    );
    let openai_request = &openai_server.requests()[0];
    let openai_body = String::from_utf8_lossy(&openai_request.body);
    assert!(openai_body.contains("user_data"));
    assert!(openai_body.contains("fixture.txt"));
    assert!(openai_body.contains("abc"));

    let anthropic_server = spawn_scripted_http_server(vec![ScriptedResponse::json(
        r#"{"id":"file_upload","type":"file","filename":"fixture.png","mime_type":"image/png","size_bytes":3,"downloadable":false}"#,
    )])
    .await;
    let anthropic = WireProvider::Anthropic.build(anthropic_server.base_url());
    let uploaded = anthropic
        .upload_file(
            FileUploadRequest::new("fixture.png", "image/png", vec![1, 2, 3])
                .expect("Anthropic upload request should validate"),
        )
        .await
        .expect("Anthropic GA upload should normalize");
    assert_eq!(
        uploaded.input_reference,
        Some(FileReference::Provider {
            id: "file_upload".to_string(),
            media_type: Some("image/png".to_string()),
        })
    );
    let anthropic_request = &anthropic_server.requests()[0];
    assert_eq!(anthropic_request.path, "/v1/files");
    assert!(!anthropic_request.headers.contains_key("anthropic-beta"));

    let gemini_upload = spawn_scripted_http_server(vec![ScriptedResponse::json(
        r#"{"file":{"name":"files/upload","displayName":"fixture.txt","mimeType":"text/plain","sizeBytes":"3","state":"ACTIVE","uri":"https://generativelanguage.googleapis.com/v1beta/files/upload"}}"#,
    )])
    .await;
    let mut signed_upload_url = gemini_upload.base_url();
    signed_upload_url.set_query(Some("signed=fixture"));
    let gemini_start = spawn_scripted_http_server(vec![
        ScriptedResponse::json("").with_header("X-Goog-Upload-URL", signed_upload_url.as_str()),
    ])
    .await;
    let gemini = WireProvider::Gemini.build(gemini_start.base_url());
    let uploaded = gemini
        .upload_file(
            FileUploadRequest::new("fixture.txt", "text/plain", b"abc".to_vec())
                .expect("Gemini upload request should validate"),
        )
        .await
        .expect("signed cross-origin Gemini upload should normalize");
    assert_eq!(uploaded.id, "files/upload");
    assert!(matches!(
        uploaded.input_reference,
        Some(FileReference::Url { ref url, ref media_type })
            if url.as_str() == "https://generativelanguage.googleapis.com/v1beta/files/upload"
                && media_type.as_deref() == Some("text/plain")
    ));
    let upload_request = &gemini_upload.requests()[0];
    assert_eq!(upload_request.path, "/?signed=fixture");
    assert!(!upload_request.headers.contains_key("x-goog-api-key"));

    let invalid_start = spawn_scripted_http_server(vec![
        ScriptedResponse::json("").with_header(
            "X-Goog-Upload-URL",
            "ftp://example.test/upload?signature=scheme-secret-17",
        ),
        ScriptedResponse::json("").with_header(
            "X-Goog-Upload-URL",
            "https://user:password@example.test/upload?signature=userinfo-secret-29",
        ),
    ])
    .await;
    let invalid_gemini = WireProvider::Gemini.build(invalid_start.base_url());
    for (name, secret) in [
        ("unsafe-scheme.txt", "scheme-secret-17"),
        ("unsafe-userinfo.txt", "userinfo-secret-29"),
    ] {
        let error = invalid_gemini
            .upload_file(
                FileUploadRequest::new(name, "text/plain", b"abc".to_vec())
                    .expect("fixture upload should validate"),
            )
            .await
            .expect_err("unsafe Gemini upload URL should be rejected");
        assert!(matches!(
            &error,
            error if matches!(error.kind(), ErrorKind::InvalidResponse(_))
        ));
        assert!(!error.to_string().contains(secret));
        assert!(!format!("{error:?}").contains(secret));
        assert!(
            error
                .native_details()
                .body
                .as_ref()
                .is_none_or(|value| !value.contains(secret))
        );
    }
    assert_eq!(invalid_start.requests().len(), 2);
}

#[tokio::test]
async fn provider_resource_ids_reject_path_injection_before_io() {
    let server = spawn_scripted_http_server(Vec::new()).await;
    let mut openai_base = server.base_url();
    openai_base.set_path("/v1");
    let openai = WireProvider::OpenAi.build(openai_base);
    assert!(matches!(
        openai.file_info("file-safe?leak=1".to_string()).await,
        Err(ref error) if error.kind() == ErrorKind::InvalidInput
    ));
    assert!(matches!(
        openai.batch_status("batch_safe/extra".to_string()).await,
        Err(ref error) if error.kind() == ErrorKind::InvalidInput
    ));

    let anthropic = WireProvider::Anthropic.build(server.base_url());
    assert!(matches!(
        anthropic.file_info("file_safe#fragment".to_string()).await,
        Err(ref error) if error.kind() == ErrorKind::InvalidInput
    ));
    assert!(matches!(
        anthropic
            .batch_status("msgbatch_safe?leak=1".to_string())
            .await,
        Err(ref error) if error.kind() == ErrorKind::InvalidInput
    ));

    let gemini = WireProvider::Gemini.build(server.base_url());
    assert!(matches!(
        gemini.file_info("files/safe?leak=1".to_string()).await,
        Err(ref error) if error.kind() == ErrorKind::InvalidInput
    ));
    assert!(matches!(
        gemini.batch_status("batches/safe/extra".to_string()).await,
        Err(ref error) if error.kind() == ErrorKind::InvalidInput
    ));
    assert!(matches!(
        gemini.model(ModelId::new("models/gemini?leak=1").expect("model id should validate")),
        Err(ref error) if error.kind() == ErrorKind::InvalidInput
    ));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn anthropic_model_and_ga_file_lists_use_their_native_pagination() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(
            r#"{
                "data": [{"id": "claude-next", "type": "model"}],
                "first_id": "claude-next",
                "last_id": "claude-next",
                "has_more": true
            }"#,
        ),
        ScriptedResponse::json(
            r#"{
                "data": [{"id": "file_next", "type": "file", "filename": "next.txt"}],
                "next_page": "file-page-next"
            }"#,
        ),
    ])
    .await;
    let provider = WireProvider::Anthropic.build(server.base_url());

    let models = provider
        .list_models(
            ModelListRequest::default()
                .with_limit(NonZeroUsize::new(2).expect("limit must be non-zero"))
                .with_cursor("model-prev")
                .expect("model cursor should validate"),
        )
        .await
        .expect("Anthropic model page should normalize");
    assert_eq!(models.next_cursor.as_deref(), Some("claude-next"));

    let files = provider
        .list_files(
            FileListRequest::default()
                .with_limit(NonZeroUsize::new(2).expect("limit must be non-zero"))
                .with_cursor("file-prev")
                .expect("file cursor should validate"),
        )
        .await
        .expect("Anthropic file page should normalize");
    assert_eq!(files.next_cursor.as_deref(), Some("file-page-next"));

    let requests = server.requests();
    assert_eq!(requests[0].path, "/v1/models?limit=2&after_id=model-prev");
    assert_eq!(requests[1].path, "/v1/files?limit=2&page=file-prev");
    assert!(!requests[1].headers.contains_key("anthropic-beta"));
}

#[tokio::test]
async fn anthropic_provider_files_require_mime_and_render_the_exact_block_kind() {
    let server = spawn_scripted_http_server(vec![
        WireResponse::anthropic_text("msg_image", "claude-sonnet-4-6", "image accepted"),
        WireResponse::anthropic_text("msg_document", "claude-sonnet-4-6", "document accepted"),
    ])
    .await;
    let model = WireProvider::Anthropic.model_at(server.base_url(), "claude-sonnet-4-6");
    let call = FunctionCall::new(Some("toolu_file".to_string()), "inspect_file", json!({}))
        .expect("fixture function call should validate");
    let assistant = Message::assistant(Vec::new(), vec![call.clone()])
        .expect("assistant function call should validate");

    for (media_type, expected_type, id) in [
        ("image/png", "image", "file_image"),
        ("application/pdf", "document", "file_document"),
    ] {
        model
            .generate(&WireRequest::function_output(
                assistant.clone(),
                &call,
                vec![ContentPart::File {
                    reference: FileReference::Provider {
                        id: id.to_string(),
                        media_type: Some(media_type.to_string()),
                    },
                }],
            ))
            .await
            .expect("supported Anthropic provider file should render");

        let request = server
            .requests()
            .last()
            .cloned()
            .expect("rendered request should be recorded");
        let body: Value =
            serde_json::from_slice(&request.body).expect("request body should be JSON");
        let block = &body["messages"][1]["content"][0]["content"][0];
        assert_eq!(block["type"], expected_type);
        assert_eq!(block["source"], json!({"type": "file", "file_id": id}));
        assert!(!request.headers.contains_key("anthropic-beta"));
    }

    for (media_type, expected) in [(None, "invalid input"), (Some("audio/wav"), "unsupported")] {
        let result = model
            .generate(&WireRequest::function_output(
                assistant.clone(),
                &call,
                vec![ContentPart::File {
                    reference: FileReference::Provider {
                        id: "file_rejected".to_string(),
                        media_type: media_type.map(str::to_string),
                    },
                }],
            ))
            .await;
        match (expected, result) {
            ("invalid input", Err(ref error)) if error.kind() == ErrorKind::InvalidInput => {}
            ("unsupported", Err(ref error)) if error.kind() == ErrorKind::Unsupported => {}
            (_, other) => panic!("unexpected Anthropic provider-file result: {other:?}"),
        }
    }
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn gemini_file_search_validates_unreferenced_chunks_before_projection() {
    for (chunk, expected) in [
        (
            json!({"retrievedContext": {"text": "Evidence."}, "futureSource": {}}),
            "unknown source family",
        ),
        (
            json!({"retrievedContext": {}, "web": {"uri": "https://example.test"}}),
            "exactly one supported source",
        ),
        (
            json!({"retrievedContext": {}, "maps": {"uri": "https://example.test"}}),
            "location grounding",
        ),
        (json!({}), "exactly one supported source"),
        (
            json!({"retrievedContext": {"title": 42}}),
            "retrievedContext.title",
        ),
        (
            json!({"retrievedContext": {"text": []}}),
            "retrievedContext.text",
        ),
    ] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
            &json!({
                "candidates": [{
                    "content": {"role": "model", "parts": [{"text": "Source evidence."}]},
                    "finishReason": "STOP",
                    "groundingMetadata": {"groundingChunks": [chunk]}
                }],
                "modelVersion": "models/gemini-2.5-flash"
            })
            .to_string(),
        )])
        .await;
        let model = WireProvider::Gemini.model_at(server.base_url(), "models/gemini-2.5-flash");
        let request = WireRequest::user("Search source evidence.")
            .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::FileSearch {
                store_ids: vec!["fileSearchStores/fixture".to_string()],
            })])
            .expect("file-search declaration should validate");
        let error = model
            .generate(&request)
            .await
            .expect_err("unreferenced malformed grounding must not disappear in projection");
        assert!(
            matches!(&error, error if matches!(error.kind(), ErrorKind::InvalidResponse(_)) && error.native_details().message.as_deref().is_some_and(|message| message.contains(expected))),
            "wrong error for grounding chunk {chunk}: {error}"
        );
        assert_eq!(server.requests().len(), 1);
    }
}

#[tokio::test]
async fn gemini_file_search_checks_reference_bounds_before_projection() {
    use providers_kit::error::ResponseDetails;

    for segment in [Value::Null, json!({"startIndex": 0, "endIndex": 8})] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
            &json!({
                "candidates": [{
                    "content": {"role": "model", "parts": [{"text": "Evidence"}]},
                    "finishReason": "STOP",
                    "groundingMetadata": {
                        "groundingChunks": [{"retrievedContext": {"text": "Evidence"}}],
                        "groundingSupports": [{"groundingChunkIndices": [1], "segment": segment}]
                    }
                }],
                "modelVersion": "models/gemini-2.5-flash"
            })
            .to_string(),
        )])
        .await;
        let model = WireProvider::Gemini.model_at(server.base_url(), "models/gemini-2.5-flash");
        let error = model
            .generate(&WireRequest::user("Search source evidence."))
            .await
            .expect_err("every grounding support index must be validated");
        assert!(
            error.kind()
                == ErrorKind::InvalidResponse(ResponseViolation::InvalidField {
                    field: "groundingChunkIndices"
                })
                && error.response_details()
                    == Some(&ResponseDetails::Index {
                        index: 1,
                        available: 1
                    }),
            "wrong error for segment {segment}: {error}"
        );
    }
}

#[tokio::test]
async fn gemini_file_delete_requires_an_empty_json_object() {
    for (body, succeeds) in [
        ("{}\n", true),
        (" \n{ \t}\n", true),
        ("", false),
        ("null", false),
        ("[]", false),
        ("{\"deleted\":true}", false),
        ("{invalid}", false),
    ] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::json(body)]).await;
        let provider = WireProvider::Gemini.build(server.base_url());
        let result = provider
            .delete_file("files/delete-fixture".to_string())
            .await;
        if succeeds {
            result.expect("Gemini's empty JSON deletion response should succeed");
        } else {
            assert!(
                matches!(result, Err(ref error) if matches!(error.kind(), ErrorKind::InvalidResponse(_))),
                "unexpected deletion body {body:?} must fail normalization"
            );
        }
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "DELETE");
        assert_eq!(requests[0].path, "/v1beta/files/delete-fixture");
    }
}

#[tokio::test]
async fn gemini_file_search_projects_identified_results_in_generation_and_streams() {
    let native = json!({
        "candidates": [{
            "content": {"role": "model", "parts": [{"text": "Alpha beta"}]},
            "finishReason": "STOP",
            "groundingMetadata": {
                "retrievalQueries": ["source evidence"],
                "groundingChunks": [
                    {"retrievedContext": {
                        "title": "unidentified.txt",
                        "text": "Unidentified source excerpt.",
                        "fileSearchStore": "fileSearchStores/fixture"
                    }},
                    {"retrievedContext": {
                        "uri": "https://example.test/document.txt",
                        "title": "identified.txt",
                        "text": "Identified source excerpt.",
                        "fileSearchStore": "fileSearchStores/fixture"
                    }}
                ],
                "groundingSupports": [
                    {"segment": {"startIndex": 0, "endIndex": 5}, "groundingChunkIndices": [0]},
                    {"segment": {"startIndex": 6, "endIndex": 10, "text": "beta"}, "groundingChunkIndices": [1]}
                ]
            }
        }],
        "modelVersion": "models/gemini-2.5-flash"
    });
    for has_identity in [true, false] {
        let mut body = native.clone();
        if !has_identity {
            body["candidates"][0]["groundingMetadata"]["groundingChunks"][1]["retrievedContext"]
                .as_object_mut()
                .expect("retrieved context should be an object")
                .remove("uri");
        }
        let body = body.to_string();
        let server = spawn_scripted_http_server(vec![
            ScriptedResponse::json(&body),
            ScriptedResponse::sse(&format!("data: {body}\n\n")),
        ])
        .await;
        let model = WireProvider::Gemini.model_at(server.base_url(), "models/gemini-2.5-flash");
        let request = WireRequest::user("Search source evidence.")
            .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::FileSearch {
                store_ids: vec!["fileSearchStores/fixture".to_string()],
            })])
            .expect("file-search declaration should validate");

        for streaming in [false, true] {
            let response = if streaming {
                let mut stream = model
                    .generate_stream(&request)
                    .await
                    .expect("Gemini file-search stream should open");
                let response = loop {
                    if let GenerateEvent::Completed(response) = stream
                        .next()
                        .await
                        .expect("file-search stream should reach completion")
                        .expect("missing source identity should not invalidate generation")
                    {
                        break *response;
                    }
                };
                assert!(stream.next().await.is_none());
                response
            } else {
                model
                    .generate(&request)
                    .await
                    .expect("file search should normalize")
            };
            let (results, citations) = if has_identity {
                (
                    vec![providers_kit::hosted::HostedFileSearchResult {
                        file_id: "https://example.test/document.txt".to_string(),
                        file_name: Some("identified.txt".to_string()),
                        text: Some("Identified source excerpt.".to_string()),
                        score: None,
                    }],
                    vec![providers_kit::generation::Citation::File {
                        reference: FileReference::Provider {
                            id: "https://example.test/document.txt".to_string(),
                            media_type: None,
                        },
                        title: Some("identified.txt".to_string()),
                        snippet: Some("beta".to_string()),
                        start_index: Some(6),
                        end_index: Some(10),
                    }],
                )
            } else {
                (Vec::new(), Vec::new())
            };
            assert_eq!(
                response.message.content(),
                &[ContentPart::Text {
                    text: "Alpha beta".to_string()
                }]
            );
            assert_eq!(response.citations, citations);
            assert_eq!(
                response.hosted_tool_outputs,
                [HostedToolOutput::FileSearch {
                    status: None,
                    queries: vec!["source evidence".to_string()],
                    results: Some(results),
                    citations,
                }]
            );
        }
        assert_eq!(server.requests().len(), 2);
    }
}

#[tokio::test]
async fn gemini_file_search_rejects_malformed_present_identity() {
    for uri in [Value::Null, json!(17), json!(""), json!("  ")] {
        let body = json!({
            "candidates": [{
                "content": {"role": "model", "parts": [{"text": "Source evidence."}]},
                "finishReason": "STOP",
                "groundingMetadata": {
                    "groundingChunks": [{"retrievedContext": {
                        "uri": uri,
                        "title": "source.txt",
                        "text": "Source excerpt.",
                        "fileSearchStore": "fileSearchStores/fixture"
                    }}]
                }
            }],
            "modelVersion": "models/gemini-2.5-flash"
        });
        let server =
            spawn_scripted_http_server(vec![ScriptedResponse::json(&body.to_string())]).await;
        let model = WireProvider::Gemini.model_at(server.base_url(), "models/gemini-2.5-flash");
        let request = WireRequest::user("Search source evidence.")
            .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::FileSearch {
                store_ids: vec!["fileSearchStores/fixture".to_string()],
            })])
            .expect("file-search declaration should validate");
        assert!(
            matches!(
                model.generate(&request).await,
                Err(ref error) if matches!(error.kind(), ErrorKind::InvalidResponse(_))
            ),
            "present malformed URI {uri:?} must remain an invalid response"
        );
        assert_eq!(server.requests().len(), 1);
    }
}

#[tokio::test]
async fn gemini_preserves_query_only_web_search_grounding() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
        r#"{
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{"text": "The current observation is available from weather.gov."}]
                },
                "finishReason": "STOP",
                "groundingMetadata": {
                    "webSearchQueries": ["current Chicago O'Hare weather.gov observation"],
                    "searchEntryPoint": {"renderedContent": "<div>provider widget</div>"}
                }
            }],
            "modelVersion": "models/gemini-2.5-flash"
        }"#,
    )])
    .await;
    let model = WireProvider::Gemini.model_at(server.base_url(), "models/gemini-2.5-flash");

    let response = model
        .generate(
            &WireRequest::user("Search weather.gov.")
                .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::WebSearch)])
                .expect("hosted tool should validate"),
        )
        .await
        .expect("query-only Gemini grounding metadata should normalize");

    let output = response
        .hosted_tool_outputs
        .first()
        .expect("query-only grounding should retain web-search evidence");
    let HostedToolOutput::WebSearch {
        action,
        results,
        citations,
        ..
    } = output
    else {
        panic!("query-only grounding should normalize as web search");
    };
    let action = action
        .as_ref()
        .expect("provider-authored queries should retain the search action");
    let providers_kit::hosted::HostedWebSearchAction::Search { queries, .. } = action else {
        panic!("native search action");
    };
    assert_eq!(queries, &["current Chicago O'Hare weather.gov observation"]);
    assert!(results.is_empty());
    assert!(citations.is_empty());
}

#[tokio::test]
async fn gemini_function_result_media_requires_exact_model_and_mime_support() {
    let response = |model: &str| {
        ScriptedResponse::json(
            &json!({
                "candidates": [{
                    "content": {"role": "model", "parts": [{"text": "accepted"}]},
                    "finishReason": "STOP"
                }],
                "modelVersion": model
            })
            .to_string(),
        )
    };
    let models = [
        "models/gemini-3.1-pro-preview",
        "models/gemini-3-flash-preview",
    ];
    let supported = [
        ContentPart::Image {
            media_type: "image/png".to_string(),
            data: vec![1],
        },
        ContentPart::Image {
            media_type: "image/jpeg".to_string(),
            data: vec![2],
        },
        ContentPart::Image {
            media_type: "image/webp".to_string(),
            data: vec![3],
        },
        ContentPart::Document {
            media_type: "application/pdf".to_string(),
            data: vec![4],
            file_name: Some("fixture.pdf".to_string()),
        },
        ContentPart::Document {
            media_type: "text/plain".to_string(),
            data: vec![5],
            file_name: Some("fixture.txt".to_string()),
        },
    ];
    let server = spawn_scripted_http_server(
        models
            .iter()
            .flat_map(|model| (0..supported.len()).map(|_| response(model)))
            .collect(),
    )
    .await;
    let call = FunctionCall::new(Some("call-media".to_string()), "inspect", json!({}))
        .expect("fixture call should validate");
    let assistant = Message::assistant(Vec::new(), vec![call.clone()])
        .expect("assistant function call should validate");

    for model in models {
        let exact = WireProvider::Gemini.model_at(server.base_url(), model);
        for part in &supported {
            exact
                .generate(&WireRequest::function_output(
                    assistant.clone(),
                    &call,
                    vec![part.clone()],
                ))
                .await
                .expect("exact Gemini 3 function-result media should render");
        }
    }

    let recorded = server.requests();
    let expected_media = [
        "image/png",
        "image/jpeg",
        "image/webp",
        "application/pdf",
        "text/plain",
    ];
    for (request, expected) in recorded.iter().zip(expected_media.iter().cycle()) {
        let body: Value =
            serde_json::from_slice(&request.body).expect("media request should be JSON");
        assert_eq!(
            body["contents"][1]["parts"][0]["functionResponse"]["parts"][0]["inlineData"]["mimeType"],
            *expected
        );
    }

    let exact = WireProvider::Gemini.model_at(server.base_url(), models[1]);
    let unknown = WireProvider::Gemini.model_at(server.base_url(), "models/gemini-3-unknown");

    for part in [
        ContentPart::Image {
            media_type: "image/gif".to_string(),
            data: vec![1],
        },
        ContentPart::Document {
            media_type: "application/msword".to_string(),
            data: vec![1],
            file_name: Some("fixture.doc".to_string()),
        },
        ContentPart::Audio {
            media_type: "audio/wav".to_string(),
            data: vec![1],
        },
        ContentPart::ImageUrl {
            url: reqwest::Url::parse("https://example.test/image.png")
                .expect("fixture URL should parse"),
        },
        ContentPart::File {
            reference: FileReference::Provider {
                id: "files/native".to_string(),
                media_type: Some("application/pdf".to_string()),
            },
        },
    ] {
        assert!(matches!(
            exact
                .generate(&WireRequest::function_output(
                    assistant.clone(),
                    &call,
                    vec![part],
                ))
                .await,
            Err(ref error) if error.kind() == ErrorKind::Unsupported
        ));
    }
    assert!(matches!(
        unknown
            .generate(&WireRequest::function_output(
                assistant,
                &call,
                vec![supported[0].clone()],
            ))
            .await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    assert_eq!(server.requests().len(), models.len() * supported.len());
}

#[tokio::test]
async fn anthropic_replays_private_tool_use_blocks_without_reconstruction() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(
            r#"{
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": "toolu_1",
                    "name": "lookup_weather",
                    "input": {"city": "Chicago"},
                    "caller": {"type": "direct"}
                }],
                "model": "claude-wire-20260826",
                "stop_reason": "tool_use",
                "usage": {"input_tokens": 4, "output_tokens": 2}
            }"#,
        ),
        WireResponse::anthropic_text("msg_2", "claude-wire-20260826", "72 F"),
    ])
    .await;
    let model = WireProvider::Anthropic.model_at(server.base_url(), "claude-wire");
    let generation = GenerationConfig::default()
        .with_max_output_tokens(NonZeroU32::new(128).expect("token limit must be non-zero"));

    let first = model
        .generate(&WireRequest::user("Weather in Chicago?"))
        .await
        .expect("first Anthropic response should normalize");
    assert_eq!(first.finish_reason, Some(FinishReason::ToolCall));
    let serialization = serde_json::to_value(&first.message)
        .expect_err("provider continuation must not be serialized lossily");
    assert!(serialization.to_string().contains("provider continuation"));
    let call = first
        .message
        .function_calls()
        .first()
        .expect("Anthropic function call should normalize");
    model
        .generate(
            &WireRequest::function_output(
                first.message.clone(),
                call,
                vec![ContentPart::Text {
                    text: "72 F".to_string(),
                }],
            )
            .with_generation(generation)
            .expect("generation config should validate"),
        )
        .await
        .expect("Anthropic continuation should normalize");

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    let first_body: Value =
        serde_json::from_slice(&requests[0].body).expect("first body should be JSON");
    assert_eq!(first_body["max_tokens"], 1_024);
    assert_eq!(requests[1].path, "/v1/messages");
    assert_eq!(
        requests[1].headers.get("x-api-key").map(String::as_str),
        Some("anthropic-wire-key")
    );
    assert_eq!(
        requests[1]
            .headers
            .get("anthropic-version")
            .map(String::as_str),
        Some("2023-06-01")
    );
    let replay: Value =
        serde_json::from_slice(&requests[1].body).expect("second body should be JSON");
    assert_eq!(
        replay["messages"][0]["content"][0],
        json!({
            "type": "tool_use",
            "id": "toolu_1",
            "name": "lookup_weather",
            "input": {"city": "Chicago"},
            "caller": {"type": "direct"}
        })
    );
    assert_eq!(
        replay["messages"][1]["content"][0],
        json!({
            "type": "tool_result",
            "tool_use_id": "toolu_1",
            "content": "72 F",
            "is_error": false
        })
    );
}

#[tokio::test]
async fn anthropic_stream_continuation_uses_selected_model_and_requires_stop_reason() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::sse(concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_stream\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-wire-20260826\",\"stop_reason\":null,\"usage\":{\"input_tokens\":5,\"output_tokens\":0,\"cache_read_input_tokens\":3}}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"first\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7,\"output_tokens_details\":{\"thinking_tokens\":4}}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        )),
        WireResponse::anthropic_text("msg_after_stream", "claude-wire-20260826", "second"),
        ScriptedResponse::sse(concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_missing_stop\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"malformed\"}],\"model\":\"claude-wire-20260826\",\"stop_reason\":null}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        )),
    ])
    .await;
    let model = WireProvider::Anthropic.model_at(server.base_url(), "claude-wire");
    let request = WireRequest::user("stream");

    let events = model
        .generate_stream(&request)
        .await
        .expect("stream should open")
        .try_collect::<Vec<_>>()
        .await
        .expect("valid Anthropic stream should normalize");
    let GenerateEvent::Completed(response) = events.last().expect("stream should complete") else {
        panic!("last stream event should be completion");
    };
    assert_eq!(response.usage.input_tokens, Some(5));
    assert_eq!(response.usage.output_tokens, Some(7));
    assert_eq!(response.usage.cached_input_tokens, Some(3));
    assert_eq!(response.usage.reasoning_tokens, Some(4));
    assert_eq!(response.usage.total_tokens, None);
    model
        .generate(&WireRequest::continuation(
            response.message.clone(),
            "Continue.",
        ))
        .await
        .expect("stream continuation should remain bound to the selected model handle");

    let error = model
        .generate_stream(&request)
        .await
        .expect("malformed stream should open")
        .try_collect::<Vec<_>>()
        .await
        .expect_err("message_stop without stop_reason must fail");
    assert!(matches!(
        error,
        ref error if matches!(error.kind(), ErrorKind::InvalidResponse(_)) && error.native_details().message.as_deref().is_some_and(|message| message.contains("terminal stop_reason"))
    ));
}

#[tokio::test]
async fn anthropic_stream_citation_delta_completes_with_citation() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::sse(concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_citation_fixture\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-wire\",\"stop_reason\":null,\"usage\":{\"input_tokens\":4,\"output_tokens\":0}}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"IANA\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"citations_delta\",\"citation\":{\"type\":\"web_search_result_location\",\"url\":\"https://www.iana.org/help/example-domains\",\"title\":\"Example Domains\",\"cited_text\":\"IANA\",\"encrypted_index\":\"fixture-native-index\"}}}\n\n",
        "event: content_block_stop\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    ))])
    .await;
    let provider = WireProvider::Anthropic.build(server.base_url());
    let model = WireProvider::Anthropic.model(&provider, "claude-wire");
    let request = WireRequest::user("Cite the IANA example domains page.");
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        let mut stream = model.generate_stream(&request).await?;
        let events = stream.by_ref().collect::<Vec<_>>().await;
        let eof = stream.next().await.is_none();
        Ok::<_, providers_kit::KitError>((events, eof))
    })
    .await;
    let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    assert!(
        matches!(shutdown, Ok(Ok(()))),
        "shutdown failed: {shutdown:?}; stream observations: {observed:?}"
    );
    eprintln!("Anthropic fixture provider shutdown: ok");
    assert_eq!(server.requests().len(), 1);
    let (observed, eof) = observed
        .expect("Anthropic citation stream should reach EOF within ten seconds")
        .expect("Anthropic citation stream should open");
    assert!(eof, "citation stream must remain at EOF");
    let events = observed
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("documented Anthropic citation delta should complete successfully");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, GenerateEvent::Started { .. }))
            .count(),
        1
    );
    assert!(
        matches!(events.first(), Some(GenerateEvent::Started { model }) if model.as_str() == "claude-wire")
    );
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                GenerateEvent::ContentDelta { delta } => Some(delta.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        ["IANA"]
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, GenerateEvent::Completed(_)))
            .count(),
        1
    );
    let Some(GenerateEvent::Completed(response)) = events.last() else {
        panic!("Anthropic citation stream must end with its completed response");
    };
    assert_eq!(response.model.as_str(), "claude-wire");
    assert_eq!(response.finish_reason, Some(FinishReason::Stop));
    assert_eq!(
        response.message.content(),
        [ContentPart::Text {
            text: "IANA".to_owned(),
        }]
    );
    assert_eq!(
        response.citations,
        [Citation::Url {
            url: "https://www.iana.org/help/example-domains".to_owned(),
            title: Some("Example Domains".to_owned()),
            snippet: Some("IANA".to_owned()),
            start_index: None,
            end_index: None,
        }]
    );
}

#[tokio::test]
async fn anthropic_refusal_and_terminal_envelope_are_normalized_exactly() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(
            r#"{
                "id": "msg_refusal",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": "I cannot help with that."}],
                "model": "claude-wire",
                "stop_reason": "end_turn",
                "stop_details": {
                    "type": "refusal",
                    "explanation": "The request is disallowed."
                }
            }"#,
        ),
        ScriptedResponse::json(
            r#"{
                "id": "msg_invalid",
                "type": "message",
                "role": "user",
                "content": [{"type": "text", "text": "impossible"}],
                "model": "claude-wire",
                "stop_reason": "end_turn"
            }"#,
        ),
    ])
    .await;
    let model = WireProvider::Anthropic.model_at(server.base_url(), "claude-wire");
    let request = WireRequest::user("Unsafe request");

    let refusal = model
        .generate(&request)
        .await
        .expect("documented Anthropic refusal should normalize");
    assert_eq!(refusal.finish_reason, Some(FinishReason::ContentFilter));
    assert_eq!(
        refusal.message.content(),
        [ContentPart::Text {
            text: "I cannot help with that.".to_string(),
        }]
    );

    let error = model
        .generate(&request)
        .await
        .expect_err("an impossible Anthropic role must fail");
    assert!(matches!(
        error,
        ref error if matches!(error.kind(), ErrorKind::InvalidResponse(_))
    ));
}

#[tokio::test]
async fn anthropic_basic_hosted_tools_require_an_exact_supported_model() {
    let server = spawn_scripted_http_server(vec![
        WireResponse::anthropic_text("msg_web_search", "claude-sonnet-4-6", "done"),
        WireResponse::anthropic_text("msg_url_context", "claude-sonnet-4-6", "done"),
        WireResponse::anthropic_text("msg_code", "claude-sonnet-4-6", "done"),
    ])
    .await;
    let provider = WireProvider::Anthropic.build(server.base_url());
    let model = WireProvider::Anthropic.model(&provider, "claude-sonnet-4-6");

    for tool in [
        HostedTool::WebSearch,
        HostedTool::UrlContext,
        HostedTool::CodeExecution,
    ] {
        let request = WireRequest::user("Use the hosted tool.")
            .with_tools(vec![ProviderToolSpec::Hosted(tool)])
            .expect("hosted tool should validate")
            .with_generation(GenerationConfig::default().with_max_output_tokens(
                NonZeroU32::new(128).expect("token limit must be non-zero"),
            ))
            .expect("generation config should validate");
        model
            .generate(&request)
            .await
            .expect("Anthropic hosted tool should render for an exact supported model");
    }

    let file_search = WireRequest::user("Search a file.")
        .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::FileSearch {
            store_ids: vec!["store-native".to_string()],
        })])
        .expect("file-search declaration should validate");
    assert!(matches!(
        model.generate(&file_search).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));

    let unknown = WireProvider::Anthropic.model(&provider, "claude-unknown");
    let request = WireRequest::user("Search the web.")
        .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::WebSearch)])
        .expect("hosted tool should validate");
    assert!(matches!(
        unknown.generate(&request).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));

    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    let expected = [
        json!({"type": "web_search_20250305", "name": "web_search"}),
        json!({"type": "web_fetch_20250910", "name": "web_fetch"}),
        json!({"type": "code_execution_20250825", "name": "code_execution"}),
    ];
    for (request, expected) in requests.iter().zip(expected) {
        let body: Value =
            serde_json::from_slice(&request.body).expect("Anthropic request body should be JSON");
        assert_eq!(body["tools"], json!([expected]));
    }
}

#[tokio::test]
async fn openai_gemini_and_ollama_cover_the_closed_hosted_tool_vocabulary() {
    let openai_response = ScriptedResponse::json(
        r#"{"model":"gpt-4.1","status":"completed","output":[{"id":"msg","type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}"#,
    );
    let openai_server = spawn_scripted_http_server(vec![
        openai_response.clone(),
        openai_response.clone(),
        openai_response,
    ])
    .await;
    let openai = WireProvider::OpenAi.api_model(openai_server.base_url(), "gpt-4.1");
    for tool in [
        HostedTool::WebSearch,
        HostedTool::FileSearch {
            store_ids: vec!["vs_fixture".to_string()],
        },
        HostedTool::CodeExecution,
    ] {
        openai
            .generate(
                &WireRequest::user("Use the hosted tool.")
                    .with_tools(vec![ProviderToolSpec::Hosted(tool)])
                    .expect("OpenAI hosted tool should validate"),
            )
            .await
            .expect("supported OpenAI hosted tool should render");
    }
    let openai_url = WireRequest::user("Read a URL.")
        .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::UrlContext)])
        .expect("URL-context declaration should validate");
    assert!(matches!(
        openai.generate(&openai_url).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    let openai_requests = openai_server.requests();
    assert_eq!(openai_requests.len(), 3);
    let expected_openai = ["web_search", "file_search", "code_interpreter"];
    for (request, expected) in openai_requests.iter().zip(expected_openai) {
        let body: Value =
            serde_json::from_slice(&request.body).expect("OpenAI request body should be JSON");
        assert_eq!(body["tools"][0]["type"], expected);
    }

    let gemini_response = ScriptedResponse::json(
        r#"{"candidates":[{"content":{"role":"model","parts":[{"text":"done"}]},"finishReason":"STOP"}],"modelVersion":"models/gemini-2.5-flash"}"#,
    );
    let gemini_server = spawn_scripted_http_server(vec![
        gemini_response.clone(),
        gemini_response.clone(),
        gemini_response,
    ])
    .await;
    let gemini = WireProvider::Gemini.model_at(gemini_server.base_url(), "models/gemini-2.5-flash");
    for tool in [
        HostedTool::UrlContext,
        HostedTool::FileSearch {
            store_ids: vec!["fileSearchStores/fixture".to_string()],
        },
        HostedTool::CodeExecution,
    ] {
        gemini
            .generate(
                &WireRequest::user("Use the hosted tool.")
                    .with_tools(vec![ProviderToolSpec::Hosted(tool)])
                    .expect("Gemini hosted tool should validate"),
            )
            .await
            .expect("supported Gemini hosted tool should render");
    }
    let gemini_requests = gemini_server.requests();
    assert_eq!(gemini_requests.len(), 3);
    let expected_gemini = ["url_context", "fileSearch", "code_execution"];
    for (request, expected) in gemini_requests.iter().zip(expected_gemini) {
        let body: Value =
            serde_json::from_slice(&request.body).expect("Gemini request body should be JSON");
        assert!(body["tools"][0].get(expected).is_some());
    }

    let unsupported_server = spawn_scripted_http_server(Vec::new()).await;
    let ollama = WireProvider::Ollama.api_model(unsupported_server.base_url(), "llama3");
    for tool in [
        HostedTool::WebSearch,
        HostedTool::UrlContext,
        HostedTool::FileSearch {
            store_ids: vec!["store".to_string()],
        },
        HostedTool::CodeExecution,
    ] {
        let request = WireRequest::user("Use the hosted tool.")
            .with_tools(vec![ProviderToolSpec::Hosted(tool)])
            .expect("hosted tool should validate");
        assert!(matches!(
            ollama.generate(&request).await,
            Err(ref error) if error.kind() == ErrorKind::Unsupported
        ));
    }
    assert!(unsupported_server.requests().is_empty());
}

#[tokio::test]
async fn anthropic_reasoning_renders_adaptive_summary_and_haiku_alias_budget() {
    let server = spawn_scripted_http_server(vec![
        WireResponse::anthropic_text("msg_reasoning", "claude-opus-5", "done"),
        WireResponse::anthropic_text("msg_summary", "claude-opus-5", "done"),
        WireResponse::anthropic_text("msg_haiku", "claude-haiku-4-5", "done"),
    ])
    .await;
    let provider = WireProvider::Anthropic.build(server.base_url());
    let model = WireProvider::Anthropic.model(&provider, "claude-opus-5");
    let request =
        WireRequest::user("Reason carefully.")
            .with_reasoning(ReasoningConfig {
                effort: Some(ReasoningEffort::Maximum),
                ..ReasoningConfig::default()
            })
            .with_generation(GenerationConfig::default().with_max_output_tokens(
                NonZeroU32::new(1024).expect("token limit must be non-zero"),
            ))
            .expect("generation config should validate");

    model
        .generate(&request)
        .await
        .expect("adaptive reasoning request should normalize");
    model
        .generate(
            &WireRequest::user("Show the summary.").with_reasoning(ReasoningConfig {
                summary_mode: ReasoningSummaryMode::Summary,
                ..ReasoningConfig::default()
            }),
        )
        .await
        .expect("summary-only adaptive reasoning request should normalize");
    WireProvider::Anthropic
        .model(&provider, "claude-haiku-4-5")
        .generate(
            &WireRequest::user("Use a manual budget.")
                .with_reasoning(ReasoningConfig {
                    effort: None,
                    max_reasoning_tokens: Some(1_024),
                    summary_mode: ReasoningSummaryMode::Hidden,
                })
                .with_generation(GenerationConfig::default().with_max_output_tokens(
                    NonZeroU32::new(2_048).expect("token limit must be non-zero"),
                ))
                .expect("generation config should validate"),
        )
        .await
        .expect("Haiku alias manual reasoning should normalize");
    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    let adaptive: Value =
        serde_json::from_slice(&requests[0].body).expect("request body should be JSON");
    assert_eq!(adaptive["thinking"]["type"], "adaptive");
    assert!(adaptive["thinking"].get("budget_tokens").is_none());
    assert_eq!(adaptive["output_config"]["effort"], "max");
    let summary: Value =
        serde_json::from_slice(&requests[1].body).expect("request body should be JSON");
    assert_eq!(
        summary["thinking"],
        json!({"type": "adaptive", "display": "summarized"})
    );
    let haiku: Value =
        serde_json::from_slice(&requests[2].body).expect("request body should be JSON");
    assert_eq!(haiku["model"], "claude-haiku-4-5");
    assert_eq!(
        haiku["thinking"],
        json!({"type": "enabled", "budget_tokens": 1_024, "display": "omitted"})
    );
}

#[tokio::test]
async fn openai_validates_responses_controls_and_current_audio_models_before_io() {
    let mut server = spawn_scripted_http_server(Vec::new()).await;
    let mut base_url = server.base_url();
    base_url.set_path("/v1");
    let provider = WireProvider::OpenAi.build(base_url);
    let message = Message::user_text("Validate locally.").expect("message should validate");

    let model = WireProvider::OpenAi.model(&provider, "gpt-4.1");
    let stop_request = WireRequest::messages(vec![message.clone()])
        .with_generation(
            GenerationConfig::default()
                .with_stop_sequences(vec!["stop".to_string()])
                .expect("stop sequence should validate"),
        )
        .expect("request should retain its generation config");
    let stop = model
        .generate(&stop_request)
        .await
        .err()
        .map(|error| error.kind());

    let model = WireProvider::OpenAi.model(&provider, "gpt-5.4");
    let sampling_request = WireRequest::messages(vec![message.clone()])
        .with_reasoning(ReasoningConfig {
            effort: Some(ReasoningEffort::High),
            ..ReasoningConfig::default()
        })
        .with_generation(
            GenerationConfig::default()
                .with_temperature(0.2)
                .expect("temperature should validate"),
        )
        .expect("request should retain its generation config");
    let sampling = model
        .generate(&sampling_request)
        .await
        .err()
        .map(|error| error.kind());

    let model = WireProvider::OpenAi.model(&provider, "gpt-5.4-pro");
    let code_request = WireRequest::messages(vec![message.clone()])
        .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::CodeExecution)])
        .expect("hosted tool should validate");
    let code = model
        .generate(&code_request)
        .await
        .err()
        .map(|error| error.kind());

    let model = WireProvider::OpenAi.model(&provider, "gpt-5.5-pro-2026-04-23");
    let stream_request = WireRequest::messages(vec![message.clone()]);
    let stream = model
        .generate_stream(&stream_request)
        .await
        .err()
        .map(|error| error.kind());
    let audio_request = WireRequest::messages(vec![
        Message::user(vec![ContentPart::Audio {
            media_type: "audio/wav".to_string(),
            data: vec![1, 2, 3],
        }])
        .expect("audio message should validate"),
    ]);
    let unknown_audio = WireProvider::OpenAi.model(&provider, "gpt-audio-unknown");
    let unknown = unknown_audio
        .generate(&audio_request)
        .await
        .err()
        .map(|error| error.kind());
    let current = WireProvider::OpenAi
        .model(&provider, "gpt-audio-1.5")
        .generate(&audio_request)
        .await
        .err()
        .map(|error| error.kind());
    let shutdown = provider.shutdown().await;
    let settled = server.finish(Duration::from_millis(100)).await;
    assert!(shutdown.is_ok(), "provider shutdown failed: {shutdown:?}");
    assert_eq!(settled, Ok(()));
    for (request, kind) in [
        ("stop", stop),
        ("sampling", sampling),
        ("code", code),
        ("stream", stream),
        ("unknown audio", unknown),
        ("current stateless audio", current),
    ] {
        assert_eq!(kind, Some(ErrorKind::Unsupported), "{request}");
    }
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn openai_renders_documented_gpt_5_4_sampling_with_none_reasoning() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
        r#"{
            "model": "gpt-5.4",
            "status": "completed",
            "output": [{
                "id": "msg_sampling",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "ok"}]
            }]
        }"#,
    )])
    .await;
    let model = WireProvider::OpenAi.api_model(server.base_url(), "gpt-5.4");
    let request = WireRequest::user("Sample.")
        .with_reasoning(ReasoningConfig {
            effort: Some(ReasoningEffort::Minimal),
            ..ReasoningConfig::default()
        })
        .with_generation(
            GenerationConfig::default()
                .with_temperature(0.2)
                .expect("temperature should validate")
                .with_top_p(0.9)
                .expect("top_p should validate"),
        )
        .expect("request should retain its generation config");
    model
        .generate(&request)
        .await
        .expect("documented GPT-5.4 sampling controls should render");

    let requests = server.requests();
    let body: Value =
        serde_json::from_slice(&requests[0].body).expect("request body should be JSON");
    assert_eq!(body["reasoning"]["effort"], "none");
    assert!(
        (body["temperature"]
            .as_f64()
            .expect("temperature should be numeric")
            - 0.2)
            .abs()
            < 1e-6
    );
    assert!((body["top_p"].as_f64().expect("top_p should be numeric") - 0.9).abs() < 1e-6);
}

#[tokio::test]
async fn openai_embeddings_restore_caller_input_order_from_native_indices() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
        r#"{
            "object": "list",
            "model": "text-embedding-3-small",
            "data": [
                {"object": "embedding", "index": 1, "embedding": [2.0, 2.5]},
                {"object": "embedding", "index": 0, "embedding": [1.0, 1.5]}
            ],
            "usage": {"prompt_tokens": 2, "total_tokens": 2}
        }"#,
    )])
    .await;
    let model = WireProvider::OpenAi.api_model(server.base_url(), "text-embedding-3-small");
    let response = model
        .embed(
            &EmbedRequest::new(vec!["first".to_string(), "second".to_string()])
                .expect("embedding inputs should validate"),
        )
        .await
        .expect("indexed embeddings should normalize");
    assert_eq!(response.vectors, [vec![1.0, 1.5], vec![2.0, 2.5]]);
}

#[tokio::test]
async fn openai_replays_private_function_call_metadata_for_continuation() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(
            r#"{
                "model": "gpt-wire-20260826",
                "status": "completed",
                "output": [
                    {
                        "id": "rs_1",
                        "type": "reasoning",
                        "summary": [{"type": "summary_text", "text": "Check the weather."}],
                        "encrypted_content": "opaque-reasoning-state"
                    },
                    {
                        "id": "fc_1",
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "lookup_weather",
                        "arguments": "{\"city\":\"Chicago\"}"
                    }
                ],
                "usage": {
                    "input_tokens": 4,
                    "output_tokens": 2,
                    "total_tokens": 6
                }
            }"#,
        ),
        ScriptedResponse::json(
            r#"{
                "model": "gpt-wire-20260826",
                "status": "completed",
                "output": [{
                    "id": "msg_1",
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "72 F"}]
                }]
            }"#,
        ),
    ])
    .await;
    let model = WireProvider::OpenAi.api_model(server.base_url(), "gpt-wire");

    let first = model
        .generate(&WireRequest::user("Weather in Chicago?"))
        .await
        .expect("first OpenAI response should normalize");
    assert_eq!(first.finish_reason, Some(FinishReason::ToolCall));
    let serialization = serde_json::to_value(&first.message)
        .expect_err("provider continuation must not be serialized lossily");
    assert!(serialization.to_string().contains("provider continuation"));
    let call = first
        .message
        .function_calls()
        .first()
        .expect("OpenAI function call should normalize");
    assert_eq!(call.id(), Some("call_1"));
    assert_eq!(call.arguments(), &json!({"city": "Chicago"}));

    let second = model
        .generate(&WireRequest::function_output(
            first.message.clone(),
            call,
            vec![ContentPart::Text {
                text: "72 F".to_string(),
            }],
        ))
        .await
        .expect("OpenAI continuation should normalize");
    assert_eq!(second.finish_reason, Some(FinishReason::Stop));

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/v1/responses");
    assert_eq!(requests[1].path, "/v1/responses");
    assert_eq!(
        requests[1].headers.get("authorization").map(String::as_str),
        Some("Bearer openai-wire-key")
    );
    let replay: Value =
        serde_json::from_slice(&requests[1].body).expect("second body should be JSON");
    let initial: Value =
        serde_json::from_slice(&requests[0].body).expect("first body should be JSON");
    assert!(
        initial["include"]
            .as_array()
            .expect("include should be an array")
            .contains(&json!("reasoning.encrypted_content"))
    );
    assert_eq!(replay["model"], "gpt-wire");
    assert_eq!(replay["store"], false);
    assert_eq!(
        replay["input"][0],
        json!({
            "id": "rs_1",
            "type": "reasoning",
            "summary": [{"type": "summary_text", "text": "Check the weather."}],
            "encrypted_content": "opaque-reasoning-state"
        })
    );
    assert_eq!(
        replay["input"][1],
        json!({
            "id": "fc_1",
            "type": "function_call",
            "call_id": "call_1",
            "name": "lookup_weather",
            "arguments": "{\"city\":\"Chicago\"}"
        })
    );
    assert_eq!(
        replay["input"][2],
        json!({
            "type": "function_call_output",
            "call_id": "call_1",
            "output": "72 F"
        })
    );
}

#[tokio::test]
async fn openai_stream_terminal_statuses_and_hosted_lifecycle_are_normalized() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::sse(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-wire-20260826\"}}\n\n",
            "data: {\"type\":\"response.queued\"}\n\n",
            "data: {\"type\":\"response.reasoning_summary_part.added\"}\n\n",
            "data: {\"type\":\"response.reasoning_summary_part.done\"}\n\n",
            "data: {\"type\":\"response.web_search_call.in_progress\"}\n\n",
            "data: {\"type\":\"response.web_search_call.searching\"}\n\n",
            "data: {\"type\":\"response.web_search_call.completed\"}\n\n",
            "data: {\"type\":\"response.incomplete\",\"response\":{\"model\":\"gpt-wire-20260826\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"output\":[{\"id\":\"msg_stream\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"partial\"}]}]}}\n\n",
        )),
        ScriptedResponse::json(
            r#"{
                "model": "gpt-wire-20260826",
                "status": "completed",
                "output": [{
                    "id": "msg_after_stream",
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "continued"}]
                }]
            }"#,
        ),
        ScriptedResponse::sse(
            "data: {\"type\":\"response.failed\",\"response\":{\"model\":\"gpt-wire-20260826\",\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"native failure\"}}}\n\n",
        ),
    ])
    .await;
    let model = WireProvider::OpenAi.api_model(server.base_url(), "gpt-wire");
    let request = WireRequest::user("stream");

    let events = model
        .generate_stream(&request)
        .await
        .expect("incomplete stream should open")
        .try_collect::<Vec<_>>()
        .await
        .expect("documented lifecycle events should normalize");
    assert!(matches!(
        events.first(),
        Some(GenerateEvent::Started { .. })
    ));
    assert!(matches!(
        events.last(),
        Some(GenerateEvent::Completed(response))
            if response.finish_reason == Some(FinishReason::Length)
    ));
    let GenerateEvent::Completed(response) = events.last().expect("stream should complete") else {
        panic!("last stream event should be completion");
    };
    model
        .generate(&WireRequest::continuation(
            response.message.clone(),
            "Continue.",
        ))
        .await
        .expect("stream continuation should remain bound to the selected model handle");

    let error = model
        .generate_stream(&request)
        .await
        .expect("failed stream should open")
        .next()
        .await
        .expect("failed stream should yield an error")
        .expect_err("native failure must not synthesize completion");
    assert!(matches!(
        error,
        ref error if error.kind() == ErrorKind::Internal
            && error.native_details().code.as_deref() == Some("server_error")
            && error.native_details().message.as_deref() == Some("native failure")
    ));
}

#[tokio::test]
async fn gemini_replays_thought_signature_on_the_exact_function_call_part() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(
            r#"{
                "candidates": [{
                    "content": {
                        "role": "model",
                        "parts": [{
                            "functionCall": {
                                "id": "call_2",
                                "name": "lookup_weather",
                                "args": {"city": "Chicago"}
                            },
                            "thoughtSignature": "signed-provider-state"
                        }]
                    },
                    "finishReason": "STOP"
                }],
                "usageMetadata": {
                    "promptTokenCount": 4,
                    "candidatesTokenCount": 2,
                    "totalTokenCount": 6
                },
                "modelVersion": "gemini-3-flash-preview"
            }"#,
        ),
        ScriptedResponse::json(
            r#"{
                "candidates": [{
                    "content": {
                        "role": "model",
                        "parts": [{"text": "72 F"}]
                    },
                    "finishReason": "STOP"
                }],
                "modelVersion": "gemini-3-flash-preview"
            }"#,
        ),
    ])
    .await;
    let model = WireProvider::Gemini.model_at(server.base_url(), "models/gemini-3-flash-preview");

    let first = model
        .generate(&WireRequest::user("Weather in Chicago?"))
        .await
        .expect("first Gemini response should normalize");
    assert_eq!(first.model.as_str(), "models/gemini-3-flash-preview");
    let call = first
        .message
        .function_calls()
        .first()
        .expect("Gemini function call should normalize");
    assert_eq!(first.finish_reason, Some(FinishReason::ToolCall));
    let serialization = serde_json::to_value(&first.message)
        .expect_err("provider continuation must not be serialized lossily");
    assert!(serialization.to_string().contains("provider continuation"));
    assert_eq!(call.id(), Some("call_2"));

    let result_content = vec![
        ContentPart::Json {
            value: json!({"temperature_f": 72}),
        },
        ContentPart::Image {
            media_type: "image/png".to_string(),
            data: vec![0, 1, 2],
        },
        ContentPart::Document {
            media_type: "application/pdf".to_string(),
            data: vec![3, 4, 5],
            file_name: Some("forecast.pdf".to_string()),
        },
    ];
    model
        .generate(&WireRequest::function_output(
            first.message.clone(),
            call,
            result_content,
        ))
        .await
        .expect("Gemini continuation should normalize");

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1].path,
        "/v1beta/models/gemini-3-flash-preview:generateContent"
    );
    assert_eq!(
        requests[1]
            .headers
            .get("x-goog-api-key")
            .map(String::as_str),
        Some("gemini-wire-key")
    );
    let replay: Value =
        serde_json::from_slice(&requests[1].body).expect("second body should be JSON");
    assert_eq!(
        replay["contents"][0]["parts"][0],
        json!({
            "functionCall": {
                "id": "call_2",
                "name": "lookup_weather",
                "args": {"city": "Chicago"}
            },
            "thoughtSignature": "signed-provider-state"
        })
    );
    assert_eq!(
        replay["contents"][1]["parts"][0],
        json!({
            "functionResponse": {
                "id": "call_2",
                "name": "lookup_weather",
                "response": {
                    "result": {"temperature_f": 72}
                },
                "parts": [
                    {"inlineData": {"mimeType": "image/png", "data": "AAEC"}},
                    {"inlineData": {"mimeType": "application/pdf", "data": "AwQF"}}
                ]
            }
        })
    );

    for content in [
        ContentPart::ImageUrl {
            url: server.base_url(),
        },
        ContentPart::File {
            reference: FileReference::Url {
                url: server.base_url(),
                media_type: Some("image/png".to_string()),
            },
        },
    ] {
        assert!(matches!(
            model
                .generate(&WireRequest::function_output(
                    first.message.clone(),
                    call,
                    vec![content],
                ))
                .await,
            Err(ref error) if error.kind() == ErrorKind::Unsupported
        ));
    }
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn gemini_stream_replays_every_raw_part_with_its_thought_signature() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::sse(concat!(
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"repeat\",\"thought\":true}]}}],\"modelVersion\":\"gemini-wire-001\"}\n\n",
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"repeat\",\"thought\":true}]}}],\"modelVersion\":\"gemini-wire-001\"}\n\n",
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Checking \"}]}}],\"modelVersion\":\"gemini-wire-001\"}\n\n",
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"call_stream\",\"name\":\"lookup_weather\",\"args\":{\"city\":\"Chicago\"}},\"thoughtSignature\":\"stream-signed-state\"}]}}],\"modelVersion\":\"gemini-wire-001\"}\n\n",
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"complete\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":4,\"candidatesTokenCount\":2,\"totalTokenCount\":6},\"modelVersion\":\"gemini-wire-001\"}\n\n",
        )),
        ScriptedResponse::json(
            r#"{
                "candidates": [{
                    "content": {"role": "model", "parts": [{"text": "72 F"}]},
                    "finishReason": "STOP"
                }],
                "modelVersion": "gemini-wire-001"
            }"#,
        ),
    ])
    .await;
    let model = WireProvider::Gemini.model_at(server.base_url(), "models/gemini-wire");

    let mut stream = model
        .generate_stream(&WireRequest::user("Weather in Chicago?"))
        .await
        .expect("Gemini stream should open");
    let mut completed = None;
    let mut thinking_deltas = Vec::new();
    while let Some(event) = stream.next().await {
        match event.expect("stream event should normalize") {
            GenerateEvent::ThinkingDelta { delta } => thinking_deltas.push(delta),
            GenerateEvent::Completed(response) => completed = Some(*response),
            _ => {}
        }
    }
    assert_eq!(thinking_deltas, ["repeat", "repeat"]);
    let first = completed.expect("Gemini stream should complete");
    assert_eq!(first.model.as_str(), "models/gemini-wire");
    assert_eq!(first.finish_reason, Some(FinishReason::ToolCall));
    assert_eq!(
        first.message.content(),
        vec![
            ContentPart::Thinking {
                text: Some("repeatrepeat".to_string()),
            },
            ContentPart::Text {
                text: "Checking complete".to_string(),
            },
        ]
    );
    let call = first
        .message
        .function_calls()
        .first()
        .expect("streamed function call should be retained");
    assert_eq!(call.id(), Some("call_stream"));
    let serialization = serde_json::to_value(&first.message)
        .expect_err("stream continuation must not be serialized lossily");
    assert!(serialization.to_string().contains("provider continuation"));

    model
        .generate(&WireRequest::function_output(
            first.message.clone(),
            call,
            vec![ContentPart::Json {
                value: json!({"temperature_f": 72}),
            }],
        ))
        .await
        .expect("Gemini continuation should normalize");

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    let replay: Value =
        serde_json::from_slice(&requests[1].body).expect("second body should be JSON");
    assert_eq!(
        replay["contents"][0]["parts"],
        json!([
            {"text": "repeat", "thought": true},
            {"text": "repeat", "thought": true},
            {"text": "Checking "},
            {
                "functionCall": {
                    "id": "call_stream",
                    "name": "lookup_weather",
                    "args": {"city": "Chicago"}
                },
                "thoughtSignature": "stream-signed-state"
            },
            {"text": "complete"}
        ])
    );
}

#[tokio::test]
async fn gemini_stream_terminal_without_parts_completes_accumulated_text() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::sse(concat!(
        "data: {\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"fixture \"}]}}],\"modelVersion\":\"gemini-wire-001\"}\n\n",
        "data: {\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"answer\"}]}}],\"modelVersion\":\"gemini-wire-001\"}\n\n",
        "data: {\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\"},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":4,\"candidatesTokenCount\":2,\"totalTokenCount\":6},\"modelVersion\":\"gemini-wire-001\"}\n\n",
    ))])
    .await;
    let provider = WireProvider::Gemini.build(server.base_url());
    let model = WireProvider::Gemini.model(&provider, "models/gemini-wire");
    let request = WireRequest::user("Return fixture answer.");
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        let mut stream = model.generate_stream(&request).await?;
        let events = stream.by_ref().collect::<Vec<_>>().await;
        let eof = stream.next().await.is_none();
        Ok::<_, providers_kit::KitError>((events, eof))
    })
    .await;
    let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    assert!(
        matches!(shutdown, Ok(Ok(()))),
        "shutdown failed: {shutdown:?}; stream observations: {observed:?}"
    );
    eprintln!("Gemini fixture provider shutdown: ok");
    assert_eq!(server.requests().len(), 1);
    let (observed, eof) = observed
        .expect("Gemini terminal stream should reach EOF within ten seconds")
        .expect("Gemini terminal stream should open");
    assert!(eof, "terminal stream must remain at EOF");
    let events = observed
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("Gemini terminal frame without new parts should complete accumulated text");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, GenerateEvent::Started { .. }))
            .count(),
        1
    );
    assert!(
        matches!(events.first(), Some(GenerateEvent::Started { model }) if model.as_str() == "models/gemini-wire")
    );
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                GenerateEvent::ContentDelta { delta } => Some(delta.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        ["fixture ", "answer"]
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, GenerateEvent::Completed(_)))
            .count(),
        1
    );
    let Some(GenerateEvent::Completed(response)) = events.last() else {
        panic!("Gemini terminal stream must end with its completed response");
    };
    assert_eq!(response.model.as_str(), "models/gemini-wire");
    assert_eq!(response.finish_reason, Some(FinishReason::Stop));
    assert_eq!(
        response.message.content(),
        [ContentPart::Text {
            text: "fixture answer".to_owned(),
        }]
    );
    assert_eq!(
        response.usage,
        Usage {
            input_tokens: Some(4),
            output_tokens: Some(2),
            total_tokens: Some(6),
            ..Usage::default()
        }
    );
}

#[tokio::test]
async fn ollama_midstream_error_is_provider_failure_then_eof() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::chunks(
        200,
        "application/x-ndjson",
        vec![
            b"{\"model\":\"ollama-wire\",\"message\":{\"role\":\"assistant\",\"content\":\"partial\"},\"done\":false}\n".to_vec(),
            b"{\"error\":\"native stream failure\"}\n".to_vec(),
        ],
    )])
    .await;
    let model = WireProvider::Ollama.api_model(server.base_url(), "ollama-wire");
    let request = WireRequest::user("stream");
    let mut stream = model
        .generate_stream(&request)
        .await
        .expect("Ollama stream should open");
    let mut saw_partial = false;
    let error = loop {
        match stream.next().await {
            Some(Ok(GenerateEvent::ContentDelta { delta })) => saw_partial |= delta == "partial",
            Some(Ok(_)) => {}
            Some(Err(error)) => break error,
            None => panic!("Ollama error envelope must fail the stream"),
        }
    };
    assert!(saw_partial);
    assert!(matches!(
        error,
        ref error if error.kind() == ErrorKind::ProviderRejected
            && error.native_details().code.is_none()
            && error.native_details().message.as_deref() == Some("native stream failure")
    ));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn framed_stream_bounds_utf8_frames_and_terminal_output() {
    let completed = format!(
        "data: {}\n\n",
        json!({
            "type": "response.completed",
            "response": {
                "model": "gpt-wire",
                "status": "completed",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "😀"}]
                }]
            }
        })
    );
    let split = completed
        .find('😀')
        .expect("fixture should contain UTF-8 text")
        + 1;
    let mut invalid_suffix = vec![0xff];
    invalid_suffix.extend_from_slice(b"\"}\n\n");
    let keepalive = format!(": {}\n\n", "x".repeat(1024 * 1024 - 4)).into_bytes();
    let mut long_stream = vec![keepalive; 33];
    long_stream.push(completed.as_bytes().to_vec());
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::chunks(
            200,
            "text/event-stream",
            vec![
                completed.as_bytes()[..split].to_vec(),
                completed.as_bytes()[split..].to_vec(),
            ],
        ),
        ScriptedResponse::chunks(
            200,
            "text/event-stream",
            vec![b"data: {\"text\":\"".to_vec(), invalid_suffix],
        ),
        ScriptedResponse::chunks(
            200,
            "text/event-stream",
            vec![b"data: ".to_vec(), vec![b'x'; 8 * 1024 * 1024]],
        ),
        ScriptedResponse::chunks(200, "text/event-stream", long_stream),
        ScriptedResponse::sse(&format!("{completed}data: {{malformed}}\n\n")),
    ])
    .await;
    let model = WireProvider::OpenAi.api_model(server.base_url(), "gpt-wire");
    let request = WireRequest::user("stream");

    let mut stream = model
        .generate_stream(&request)
        .await
        .expect("split UTF-8 stream should open");
    let completed_event = stream
        .next()
        .await
        .expect("split UTF-8 stream should complete")
        .expect("split UTF-8 should remain valid");
    let GenerateEvent::Completed(response) = completed_event else {
        panic!("split UTF-8 stream should yield its completed response");
    };
    assert_eq!(
        response.message.content(),
        &[ContentPart::Text {
            text: "😀".to_string(),
        }]
    );
    assert!(stream.next().await.is_none());

    for (expected, opening) in [
        ("invalid UTF-8", "invalid UTF-8"),
        ("frame exceeded", "oversized frame"),
    ] {
        let mut stream = model
            .generate_stream(&request)
            .await
            .unwrap_or_else(|error| panic!("{opening} stream should open: {error}"));
        let error = stream
            .next()
            .await
            .unwrap_or_else(|| panic!("{opening} stream should yield one error"))
            .expect_err("invalid stream should fail");
        assert!(
            matches!(&error, error if matches!(error.kind(), ErrorKind::InvalidResponse(_)) && error.native_details().message.as_deref().is_some_and(|message| message.contains(expected))),
            "{opening} returned the wrong error: {error}"
        );
        assert!(
            stream.next().await.is_none(),
            "{opening} yielded after error"
        );
    }

    let events = model
        .generate_stream(&request)
        .await
        .expect("long keepalive stream should open")
        .collect::<Vec<_>>()
        .await;
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], Ok(GenerateEvent::Completed(_))));

    let events = model
        .generate_stream(&request)
        .await
        .expect("terminal stream should open")
        .collect::<Vec<_>>()
        .await;
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], Ok(GenerateEvent::Completed(_))));
}

#[tokio::test]
async fn retained_stream_state_is_bounded_independently_of_transport_bytes() {
    let text = "x".repeat(2 * 1024 * 1024);
    let chunks = (0..17)
        .map(|_| {
            format!(
                "{}\n",
                json!({
                    "model": "bounded-state-model",
                    "message": {"role": "assistant", "content": text},
                    "done": false
                })
            )
            .into_bytes()
        })
        .collect();
    let server = spawn_scripted_http_server(vec![ScriptedResponse::chunks(
        200,
        "text/event-stream",
        chunks,
    )])
    .await;
    let model = WireProvider::Ollama.api_model(server.base_url(), "bounded-state-model");
    let request = WireRequest::user("stream");
    let mut stream = model
        .generate_stream(&request)
        .await
        .expect("bounded stream should open");

    let error = loop {
        match stream.next().await {
            Some(Ok(_)) => {}
            Some(Err(error)) => break error,
            None => panic!("oversized normalized state ended without an error"),
        }
    };
    assert!(matches!(
        &error,
        error if matches!(error.kind(), ErrorKind::InvalidResponse(_)) && error.native_details().message.as_deref().is_some_and(|message| message.contains("normalized stream state"))
    ));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn openai_web_search_projects_urls_and_replays_native_api_feeds() {
    for (sources, expected) in [
        (
            json!([
                {"type": "api", "name": "oai-time"},
                {"type": "url", "url": "https://example.test/first", "title": "First"},
                {"type": "api", "name": "oai-weather"},
                {"type": "url", "url": "https://example.test/second"}
            ]),
            vec![
                ("https://example.test/first", Some("First")),
                ("https://example.test/second", None),
            ],
        ),
        (json!([{"type": "api", "name": "oai-time"}]), Vec::new()),
    ] {
        let native_output = vec![
            json!({
                "id": "ws_sources",
                "type": "web_search_call",
                "status": "completed",
                "action": {
                    "type": "search",
                    "query": "current UTC date",
                    "queries": ["current UTC date"],
                    "sources": sources
                }
            }),
            json!({
                "id": "msg_sources",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Source-backed answer."}]
            }),
        ];
        let native_response = json!({
            "model": "gpt-4.1",
            "status": "completed",
            "output": native_output
        });
        for streaming in [false, true] {
            let first = if streaming {
                ScriptedResponse::sse(&format!(
                    "data: {}\n\n",
                    json!({"type": "response.completed", "response": native_response})
                ))
            } else {
                ScriptedResponse::json(&native_response.to_string())
            };
            let server = spawn_scripted_http_server(vec![
                first,
                ScriptedResponse::json(
                    &json!({
                        "model": "gpt-4.1",
                        "status": "completed",
                        "output": [{
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "Continued."}]
                        }]
                    })
                    .to_string(),
                ),
            ])
            .await;
            let model = WireProvider::OpenAi.api_model(server.base_url(), "gpt-4.1");
            let request = WireRequest::user("Find the current UTC date.")
                .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::WebSearch)])
                .expect("web-search declaration should validate");
            let response = if streaming {
                let mut stream = model
                    .generate_stream(&request)
                    .await
                    .expect("web-search stream should open");
                let GenerateEvent::Completed(response) = stream
                    .next()
                    .await
                    .expect("web-search stream should complete")
                    .expect("API feeds should normalize in the completed stream")
                else {
                    panic!("terminal response should yield Completed");
                };
                assert!(stream.next().await.is_none());
                *response
            } else {
                model
                    .generate(&request)
                    .await
                    .expect("API feeds should normalize in generation")
            };
            let [
                HostedToolOutput::WebSearch {
                    status,
                    action: Some(action),
                    results,
                    citations,
                },
            ] = response.hosted_tool_outputs.as_slice()
            else {
                panic!("response should preserve one hosted web-search output");
            };
            assert_eq!(
                status,
                &Some(providers_kit::hosted::HostedToolStatus::Completed)
            );
            let providers_kit::hosted::HostedWebSearchAction::Search {
                query,
                queries,
                sources,
            } = action
            else {
                panic!("native search action");
            };
            assert_eq!(query.as_deref(), Some("current UTC date"));
            assert_eq!(queries, &["current UTC date"]);
            assert_eq!(
                sources
                    .iter()
                    .map(|source| (source.url.as_str(), source.title.as_deref()))
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(results.is_empty());
            assert!(citations.is_empty());
            assert_eq!(
                response.message.content(),
                &[ContentPart::Text {
                    text: "Source-backed answer.".to_string()
                }]
            );
            model
                .generate(&WireRequest::continuation(response.message, "Continue."))
                .await
                .expect("native web-search continuation should render");

            let requests = server.requests();
            assert_eq!(requests.len(), 2);
            let initial: Value =
                serde_json::from_slice(&requests[0].body).expect("initial body should be JSON");
            assert!(
                initial["include"]
                    .as_array()
                    .expect("include should be an array")
                    .contains(&json!("web_search_call.action.sources"))
            );
            let continued: Value = serde_json::from_slice(&requests[1].body)
                .expect("continuation body should be JSON");
            let replay = continued["input"]
                .as_array()
                .expect("continuation input should be an array");
            assert_eq!(replay.len(), native_output.len() + 1);
            assert_eq!(&replay[..native_output.len()], native_output.as_slice());
        }
    }
}

#[tokio::test]
async fn openai_web_search_rejects_malformed_sources_after_valid_api_feeds() {
    for (source, field) in [
        (json!({"type": "api"}), "name"),
        (json!({"type": "api", "name": null}), "name"),
        (json!({"type": "api", "name": 42}), "name"),
        (json!({"type": "api", "name": ""}), "name"),
        (json!({"type": "api", "name": " \t\n"}), "name"),
        (json!({"type": "url"}), "url"),
        (json!({"type": "url", "url": null}), "url"),
        (json!({"type": "url", "url": 42}), "url"),
        (json!({"type": "unknown", "name": "oai-time"}), "url"),
        (json!({"name": "oai-time"}), "url"),
        (json!(null), "object"),
    ] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
            &json!({
                "model": "gpt-4.1",
                "status": "completed",
                "output": [{
                    "id": "ws_malformed",
                    "type": "web_search_call",
                    "status": "completed",
                    "action": {
                        "type": "search",
                        "sources": [{"type": "api", "name": "oai-time"}, source]
                    }
                }]
            })
            .to_string(),
        )])
        .await;
        let model = WireProvider::OpenAi.api_model(server.base_url(), "gpt-4.1");
        let request = WireRequest::user("Search the web.")
            .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::WebSearch)])
            .expect("web-search declaration should validate");
        let error = model
            .generate(&request)
            .await
            .expect_err("a valid API feed must not hide a malformed later source");
        assert!(
            matches!(&error, error if matches!(error.kind(), ErrorKind::InvalidResponse(_)) && error.native_details().message.as_deref().is_some_and(|message| message.contains("action.sources[1]") && message.contains(field))),
            "wrong error for source {source}: {error}"
        );
        assert_eq!(server.requests().len(), 1);
    }
}

#[tokio::test]
async fn openai_download_preserves_http_rejections_without_classifying_prose() {
    for (status, message, _formerly_unsupported) in [
        (
            400,
            "Not allowed to download files of purpose: user_data",
            true,
        ),
        (400, "File is not ready", false),
        (
            401,
            "Not allowed to download files of purpose: user_data",
            false,
        ),
        (
            400,
            "Not allowed to download files of purpose: unknown",
            false,
        ),
    ] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::error(
            status,
            &json!({"error": {"message": message, "type": "invalid_request_error"}}).to_string(),
        )])
        .await;
        let provider = WireProvider::OpenAi.build(server.base_url());
        let error = provider
            .download_file(
                "file-fixture".to_string(),
                NonZeroUsize::new(4_096).unwrap(),
            )
            .await
            .expect_err("native rejection should remain an error");
        assert_eq!(
            error.kind(),
            if status == 401 {
                ErrorKind::Authentication
            } else {
                ErrorKind::InvalidRequest
            }
        );
        assert_eq!(error.http_status(), Some(status));
        assert_eq!(error.native_details().message.as_deref(), Some(message));
        assert!(
            error
                .native_details()
                .body
                .as_ref()
                .is_some_and(|body| body.contains(message))
        );
        assert_eq!(server.requests().len(), 1);
    }
}

#[tokio::test]
async fn ollama_preserves_optional_call_ids_in_generation_streams_and_results() {
    for id in [Some("call_native_17"), None] {
        let mut call = json!({
            "function": {"index": 0, "name": "strict_lookup", "arguments": {"value": "fixture"}}
        });
        if let Some(id) = id {
            call["id"] = json!(id);
        }
        let native = json!({
            "model": "ollama-wire",
            "message": {"role": "assistant", "content": "", "tool_calls": [call]},
            "done": true,
            "done_reason": "stop"
        });
        for streaming in [false, true] {
            let first = if streaming {
                ScriptedResponse::chunks(
                    200,
                    "application/x-ndjson",
                    vec![format!("{native}\n").into_bytes()],
                )
            } else {
                ScriptedResponse::json(&native.to_string())
            };
            let server = spawn_scripted_http_server(vec![
                first,
                ScriptedResponse::json(
                    &json!({
                        "model": "ollama-wire",
                        "message": {"role": "assistant", "content": "strict-live-result-17"},
                        "done": true,
                        "done_reason": "stop"
                    })
                    .to_string(),
                ),
            ])
            .await;
            let model = WireProvider::Ollama.api_model(server.base_url(), "ollama-wire");
            let request = WireRequest::user("Call strict_lookup.");
            let response = if streaming {
                let mut events = model
                    .generate_stream(&request)
                    .await
                    .expect("Ollama function-call stream should open")
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("Ollama streamed calls should normalize");
                let Some(GenerateEvent::Completed(response)) = events.pop() else {
                    panic!("Ollama function-call stream should complete");
                };
                *response
            } else {
                model
                    .generate(&request)
                    .await
                    .expect("Ollama calls should normalize")
            };
            let [call] = response.message.function_calls() else {
                panic!("Ollama should return one function call");
            };
            assert_eq!(call.id(), id);
            assert_eq!(call.name(), "strict_lookup");
            assert_eq!(call.arguments(), &json!({"value": "fixture"}));
            model
                .generate(&WireRequest::function_output(
                    response.message.clone(),
                    call,
                    vec![ContentPart::Text {
                        text: "strict-live-result-17".to_string(),
                    }],
                ))
                .await
                .expect("Ollama function results should render");

            let requests = server.requests();
            assert_eq!(requests.len(), 2);
            let replay: Value = serde_json::from_slice(&requests[1].body)
                .expect("Ollama continuation request should be JSON");
            let messages = replay["messages"]
                .as_array()
                .expect("messages should exist");
            assert_eq!(messages.len(), 2);
            assert_eq!(messages[0]["role"], "assistant");
            assert_eq!(
                messages[0]["tool_calls"][0].get("id"),
                id.map(|id| json!(id)).as_ref()
            );
            assert_eq!(
                messages[0]["tool_calls"][0]["function"]["name"],
                call.name()
            );
            assert_eq!(
                messages[0]["tool_calls"][0]["function"]["arguments"],
                *call.arguments()
            );
            assert_eq!(messages[1]["role"], "tool");
            assert_eq!(messages[1]["tool_name"], call.name());
            assert_eq!(
                messages[1].get("tool_call_id"),
                id.map(|id| json!(id)).as_ref()
            );
            assert_eq!(messages[1]["content"], "strict-live-result-17");
        }
    }
}

#[tokio::test]
async fn ollama_rejects_malformed_native_call_ids() {
    for id in [json!(42), json!(""), json!(" padded "), json!("control\n")] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
            &json!({
                "model": "ollama-wire",
                "message": {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{
                        "id": id,
                        "function": {"name": "strict_lookup", "arguments": {}}
                    }]
                },
                "done": true,
                "done_reason": "stop"
            })
            .to_string(),
        )])
        .await;
        let model = WireProvider::Ollama.api_model(server.base_url(), "ollama-wire");
        assert!(matches!(
            model
                .generate(&WireRequest::user("Call strict_lookup."))
                .await,
            Err(ref error) if matches!(error.kind(), ErrorKind::InvalidResponse(_))
        ));
        assert_eq!(server.requests().len(), 1);
    }
}

#[tokio::test]
async fn classified_http_errors_retain_native_metadata_without_logging_it() {
    for (kind, model_id, status, body, header, category, native_code, native_type, native_status) in [
        (
            WireProvider::OpenAi,
            "gpt-4.1",
            401,
            json!({"error":{"code":"invalid_api_key","type":"authentication_error","param":"input-sensitive-field","message":"sensitive-native-message"}}),
            "x-request-id",
            ErrorKind::Authentication,
            Some("invalid_api_key"),
            Some("authentication_error"),
            None,
        ),
        (
            WireProvider::Anthropic,
            "claude-wire",
            403,
            json!({"type":"error","error":{"type":"permission_error","message":"sensitive-native-message"},"request_id":"sensitive-header-request-id"}),
            "request-id",
            ErrorKind::PermissionDenied,
            None,
            Some("permission_error"),
            None,
        ),
        (
            WireProvider::Gemini,
            "models/gemini-wire",
            404,
            json!({"error":{"code":404,"status":"NOT_FOUND","message":"sensitive-native-message","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"sensitive-resource-detail","domain":"googleapis.com"}]}}),
            "x-request-id",
            ErrorKind::NotFound,
            None,
            None,
            Some("NOT_FOUND"),
        ),
        (
            WireProvider::Ollama,
            "llama3",
            400,
            json!({"error":"sensitive-native-message"}),
            "x-request-id",
            ErrorKind::InvalidRequest,
            None,
            None,
            None,
        ),
    ] {
        let server = spawn_scripted_http_server(vec![
            ScriptedResponse::error(status, &body.to_string())
                .with_header(header, "sensitive-header-request-id")
                .with_header("retry-after", "17"),
        ])
        .await;
        let mut base_url = server.base_url();
        if matches!(kind, WireProvider::Ollama) {
            base_url.set_path("/api");
        }
        let provider = kind.build(base_url);
        let model = kind.model(&provider, model_id);
        let error = model
            .generate(&WireRequest::user("fixture"))
            .await
            .expect_err("HTTP rejection must fail");
        assert_eq!(error.kind(), category);
        assert_eq!(error.http_status(), Some(status));
        assert_eq!(error.context().operation, Operation::Generate);
        assert_eq!(error.context().phase, ErrorPhase::ResponseHeaders);
        assert_eq!(error.native_details().code.as_deref(), native_code);
        assert_eq!(error.native_details().error_type.as_deref(), native_type);
        assert_eq!(
            error.native_details().native_status.as_deref(),
            native_status
        );
        assert_eq!(
            error.native_details().request_id.as_deref(),
            Some("sensitive-header-request-id")
        );
        assert_eq!(error.native_details().retry_after.as_deref(), Some("17"));
        assert_eq!(
            error.native_details().message.as_deref(),
            Some("sensitive-native-message")
        );
        assert_eq!(
            error.native_details().body.as_deref(),
            Some(body.to_string().as_str())
        );
        for rendered in [error.to_string(), format!("{error:?}")] {
            assert!(!rendered.contains("sensitive-"));
            assert!(rendered.contains("Generate"));
            assert!(rendered.contains("ResponseHeaders"));
        }
        provider
            .shutdown()
            .await
            .expect("provider closes after rejection");
    }
}

#[tokio::test]
async fn openai_classifies_specific_quota_separately_from_rate_limits_and_missing_resources() {
    let cases = [
        (
            429,
            "insufficient_quota",
            Some("insufficient_quota"),
            ErrorKind::QuotaExceeded,
        ),
        (
            429,
            "rate_limit_error",
            Some("rate_limit_exceeded"),
            ErrorKind::RateLimited,
        ),
        (
            429,
            "insufficient_quota",
            Some("credit_balance_exhausted"),
            ErrorKind::Billing,
        ),
        (404, "invalid_request_error", None, ErrorKind::NotFound),
    ];
    let responses = cases.iter().map(|(status, error_type, code, _)| ScriptedResponse::error(*status,
        &json!({"error":{"type":error_type,"code":code,"message":"fixture-message","param":"model"}}).to_string())).collect();
    let server = spawn_scripted_http_server(responses).await;
    let model = WireProvider::OpenAi.model_at(server.base_url(), "gpt-4.1");
    for (status, error_type, code, expected) in cases {
        let error = model
            .generate(&WireRequest::user("fixture"))
            .await
            .expect_err("native rejection");
        assert_eq!(error.kind(), expected);
        assert_eq!(error.http_status(), Some(status));
        assert_eq!(error.native_details().code.as_deref(), code);
        assert_eq!(
            error.native_details().error_type.as_deref(),
            Some(error_type)
        );
        assert_eq!(error.native_details().parameter.as_deref(), Some("model"));
    }
    assert_eq!(server.requests().len(), cases.len());
}

#[tokio::test]
async fn openai_standalone_stream_errors_retain_code_and_parameter_without_an_http_error() {
    let payload = json!({"type":"error","sequence_number":0,"code":"rate_limit_exceeded","message":"sensitive-stream-message","param":"input-sensitive-param"});
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::sse(&format!("event: error\ndata: {payload}\n\n"))
            .with_header("x-request-id", "sensitive-stream-request"),
    ])
    .await;
    let model = WireProvider::OpenAi.model_at(server.base_url(), "gpt-4.1");
    let mut stream = model
        .generate_stream(&WireRequest::user("fixture"))
        .await
        .expect("HTTP stream opens");
    let error = stream
        .next()
        .await
        .expect("native error emitted")
        .expect_err("remote event is an error");
    assert_eq!(error.kind(), ErrorKind::RateLimited);
    assert_eq!(error.http_status(), None);
    assert_eq!(error.context().operation, Operation::GenerateStream);
    assert_eq!(error.context().phase, ErrorPhase::Stream);
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("sensitive-stream-request")
    );
    assert_eq!(
        error.native_details().code.as_deref(),
        Some("rate_limit_exceeded")
    );
    assert_eq!(
        error.native_details().parameter.as_deref(),
        Some("input-sensitive-param")
    );
    assert_eq!(
        error.native_details().error_type,
        None,
        "event tag is not a native error subtype"
    );
    assert!(
        error
            .native_details()
            .body
            .as_ref()
            .is_some_and(|body| body.contains("sensitive-stream-message"))
    );
    assert!(!error.to_string().contains("sensitive-"));
    assert!(!format!("{error:?}").contains("sensitive-"));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn anthropic_stream_error_retains_native_type_and_body_request_correlation() {
    let payload = json!({"type":"error","error":{"type":"overloaded_error","message":"sensitive-overload"},"request_id":"sensitive-native-request"});
    let server = spawn_scripted_http_server(vec![ScriptedResponse::sse(&format!(
        "event: error\ndata: {payload}\n\n"
    ))])
    .await;
    let model = WireProvider::Anthropic.model_at(server.base_url(), "claude-wire");
    let mut stream = model
        .generate_stream(&WireRequest::user("fixture"))
        .await
        .expect("HTTP stream opens");
    let error = stream
        .next()
        .await
        .expect("native error emitted")
        .expect_err("remote error event");
    assert_eq!(error.kind(), ErrorKind::ServiceUnavailable);
    assert_eq!(error.http_status(), None);
    assert_eq!(error.context().operation, Operation::GenerateStream);
    assert_eq!(error.context().phase, ErrorPhase::Stream);
    assert_eq!(
        error.native_details().error_type.as_deref(),
        Some("overloaded_error")
    );
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("sensitive-native-request")
    );
    assert!(!format!("{error:?}").contains("sensitive-"));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn gemini_missing_batch_model_version_identifies_the_row_and_schema_field() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json(&json!({
        "name":"batches/wire", "metadata":{"state":"JOB_STATE_SUCCEEDED"},
        "parent_only": "parent-body-prefix".to_owned() + &"x".repeat(70_000),
        "response":{"inlinedResponses":{"inlinedResponses":[
            {"metadata":{"custom_id":"row-good"},"response":{"modelVersion":"models/gemini-wire","candidates":[{"content":{"role":"model","parts":[{"text":"done"}]},"finishReason":"STOP"}]}},
            {"metadata":{"custom_id":"sensitive-row-id"},"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"done"}]},"finishReason":"STOP"}]}}
        ]}}
    }).to_string()).with_header("x-request-id", "sensitive-batch-normalization-request")]).await;
    let provider = WireProvider::Gemini.build(server.base_url());
    let error = provider
        .batch_results("batches/wire".into(), NonZeroUsize::new(131_072).unwrap())
        .await
        .expect_err("missing model version invalidates its result row");
    assert_eq!(
        error.kind(),
        ErrorKind::InvalidResponse(ResponseViolation::MissingField {
            field: "modelVersion"
        })
    );
    assert_eq!(error.context().operation, Operation::BatchResults);
    assert_eq!(error.context().phase, ErrorPhase::Normalize);
    assert_eq!(error.context().row, Some(1));
    assert!(
        !error.native_details().truncated,
        "the small row diagnostic was not truncated even though the parent response was"
    );
    assert!(
        error
            .native_details()
            .body
            .as_deref()
            .is_some_and(|body| !body.contains("parent-body-prefix"))
    );
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("sensitive-batch-normalization-request")
    );
    assert_eq!(error.correlation_id(), Some("sensitive-row-id"));
    assert!(error.to_string().contains("modelVersion"));
    assert!(error.to_string().contains("row 1"));
    assert!(!error.to_string().contains("sensitive-row-id"));
    provider
        .shutdown()
        .await
        .expect("provider closes after decode error");
}

#[tokio::test]
async fn post_decode_normalization_retains_original_bounded_body_and_request_id() {
    let body = format!(
        r#"{{"native_marker":"sensitive-normalization-payload","model":"llama3","message":{{"role":"assistant","content":"done"}},"done":true,"done_reason":"unknown-native-reason","padding":"{}"}}"#,
        "x".repeat(70_000),
    );
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(&body)
            .with_header("x-request-id", "sensitive-normalization-request"),
    ])
    .await;
    let mut base_url = server.base_url();
    base_url.set_path("/api");
    let provider = WireProvider::Ollama.build(base_url);
    let model = WireProvider::Ollama.model(&provider, "llama3");
    let error = model
        .generate(&WireRequest::user("fixture"))
        .await
        .expect_err("unknown finish reason must fail after HTTP and JSON decoding succeed");
    assert_eq!(
        error.kind(),
        ErrorKind::InvalidResponse(ResponseViolation::UnknownVariant {
            field: "done_reason"
        })
    );
    assert_eq!(error.context().operation, Operation::Generate);
    assert_eq!(error.context().phase, ErrorPhase::Normalize);
    assert_eq!(error.http_status(), None);
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("sensitive-normalization-request")
    );
    let diagnostic = error
        .native_details()
        .body
        .as_deref()
        .expect("original body remains available");
    assert!(diagnostic.starts_with(r#"{"native_marker":"sensitive-normalization-payload""#));
    assert!(diagnostic.ends_with("[response body truncated]"));
    assert!(diagnostic.len() <= 65_536);
    assert!(error.native_details().truncated);
    assert!(!error.to_string().contains("sensitive-"));
    assert!(!format!("{error:?}").contains("sensitive-"));
    provider
        .shutdown()
        .await
        .expect("provider closes after normalization failure");
}

#[tokio::test]
async fn malformed_error_envelope_preserves_http_rejection_and_decode_failure() {
    let body = "sensitive-invalid-json-error-body";
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::error(500, body)
            .with_header("x-request-id", "sensitive-malformed-request"),
    ])
    .await;
    let provider = WireProvider::OpenAi.build(server.base_url());
    let model = WireProvider::OpenAi.model(&provider, "gpt-4.1");
    let error = model
        .generate(&WireRequest::user("fixture"))
        .await
        .expect_err("HTTP rejection is primary");
    assert_eq!(error.kind(), ErrorKind::Internal);
    assert_eq!(error.http_status(), Some(500));
    assert_eq!(error.context().operation, Operation::Generate);
    assert_eq!(error.context().phase, ErrorPhase::ResponseHeaders);
    assert_eq!(error.native_details().body.as_deref(), Some(body));
    assert!(!error.native_details().truncated);
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("sensitive-malformed-request")
    );
    assert_eq!(error.supplementary().len(), 1);
    assert_eq!(
        error.supplementary()[0].kind(),
        ErrorKind::InvalidResponse(ResponseViolation::MalformedJson)
    );
    assert_eq!(error.supplementary()[0].context().phase, ErrorPhase::Decode);
    assert_eq!(
        error.supplementary()[0].context().operation,
        Operation::Generate
    );
    assert!(!format!("{error:?}").contains("sensitive-"));
    provider
        .shutdown()
        .await
        .expect("provider closes after rejected response");
}

#[tokio::test]
async fn batch_download_failure_cannot_inherit_the_prior_job_response_metadata() {
    for missing_file in [false, true] {
        let failed_download = if missing_file {
            ScriptedResponse::error(
                404,
                r#"{"error":{"type":"invalid_request_error","code":"resource_not_found","message":"missing output"}}"#,
            )
        } else {
            ScriptedResponse::json("body larger than the requested byte limit")
        };
        let server = spawn_scripted_http_server(vec![
            ScriptedResponse::json(r#"{"id":"batch_wire","status":"completed","output_file_id":"file-output","parent_marker":"prior-job-body"}"#)
                .with_header("x-request-id", "prior-job-request-id"),
            failed_download,
        ]).await;
        let provider = WireProvider::OpenAi.build(server.base_url());
        let max_bytes = if missing_file {
            NonZeroUsize::new(4096).unwrap()
        } else {
            NonZeroUsize::MIN
        };
        let error = provider
            .batch_results("batch_wire".into(), max_bytes)
            .await
            .expect_err("output download must fail");
        assert_eq!(error.context().operation, Operation::BatchResults);
        assert_eq!(
            error.native_details().request_id,
            None,
            "the output request did not return an ID"
        );
        if missing_file {
            assert_eq!(error.kind(), ErrorKind::NotFound);
            assert_eq!(error.http_status(), Some(404));
            assert_eq!(error.context().phase, ErrorPhase::ResponseHeaders);
            assert!(error.native_details().body.as_deref().is_some_and(|body| {
                body.contains("missing output") && !body.contains("prior-job-body")
            }));
        } else {
            assert_eq!(
                error.kind(),
                ErrorKind::InvalidResponse(ResponseViolation::LimitExceeded { limit: 1 })
            );
            assert_eq!(error.context().phase, ErrorPhase::ReadBody);
            assert_eq!(
                error.native_details().body,
                None,
                "a pre-read size rejection cannot acquire the preceding job's body"
            );
        }
        assert_eq!(server.requests().len(), 2);
        provider
            .shutdown()
            .await
            .expect("provider closes after download rejection");
    }
}

#[tokio::test]
async fn rejected_http_status_survives_a_truncated_error_body() {
    let prefix = "{\"error\":{\"message\":\"sensitive-partial-body";
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::error(401, prefix)
            .with_header("x-request-id", "sensitive-partial-request")
            .advertised_content_length(prefix.len() + 1024),
    ])
    .await;
    let model = WireProvider::OpenAi.model_at(server.base_url(), "gpt-4.1");
    let error = model
        .generate(&WireRequest::user("fixture"))
        .await
        .expect_err("truncated rejection fails");
    assert_eq!(error.kind(), ErrorKind::Authentication);
    assert_eq!(error.http_status(), Some(401));
    assert_eq!(error.context().operation, Operation::Generate);
    assert_eq!(error.context().phase, ErrorPhase::ResponseHeaders);
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("sensitive-partial-request")
    );
    assert!(error.native_details().truncated);
    assert!(
        error
            .native_details()
            .body
            .as_ref()
            .is_some_and(|body| body.starts_with(prefix))
    );
    assert_eq!(error.supplementary().len(), 2);
    assert!(matches!(
        error.supplementary()[1].kind(),
        ErrorKind::InvalidResponse(_)
    ));
    assert_eq!(error.supplementary()[1].context().phase, ErrorPhase::Decode);
    assert!(matches!(
        error.supplementary()[1].response_details(),
        Some(providers_kit::error::ResponseDetails::Json { .. })
    ));
    assert_eq!(error.supplementary()[0].kind(), ErrorKind::Transport);
    assert_eq!(
        error.supplementary()[0].context().phase,
        ErrorPhase::ReadBody
    );
    assert_eq!(
        error.supplementary()[0].context().operation,
        Operation::Generate
    );
    assert!(!error.to_string().contains("sensitive-"));
    assert!(!format!("{error:?}").contains("sensitive-"));
}

#[tokio::test]
async fn gemini_error_info_refines_classification_without_rewriting_native_status() {
    for (reason, expected) in [
        ("API_KEY_INVALID", ErrorKind::Authentication),
        ("UNRECOGNIZED_REASON", ErrorKind::InvalidRequest),
    ] {
        let body = json!({"error":{
            "code":400,"status":"INVALID_ARGUMENT","message":"sensitive-native-message",
            "details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":reason,"domain":"googleapis.com","metadata":{"service":"sensitive-native-service"}}]
        }});
        let server =
            spawn_scripted_http_server(vec![ScriptedResponse::error(400, &body.to_string())]).await;
        let model = WireProvider::Gemini.model_at(server.base_url(), "models/gemini-wire");
        let error = model
            .generate(&WireRequest::user("fixture"))
            .await
            .expect_err("remote rejection");
        assert_eq!(error.kind(), expected);
        assert_eq!(error.http_status(), Some(400));
        assert_eq!(
            error.native_details().native_status.as_deref(),
            Some("INVALID_ARGUMENT")
        );
        assert_eq!(error.native_details().reported_http_status, Some(400));
        assert_eq!(error.native_details().code, None);
        assert!(
            error
                .native_details()
                .details
                .iter()
                .any(|detail| matches!(detail,
            providers_kit::error::NativeDetail::Reason { domain, reason: native }
                if domain == "googleapis.com" && native == reason))
        );
        assert_eq!(
            error.native_details().body.as_deref(),
            Some(body.to_string().as_str())
        );
        assert!(!format!("{error:?}").contains("sensitive-"));
    }
}

#[tokio::test]
async fn missing_installed_ollama_model_is_not_found_after_catalog_lookup() {
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(r#"{"models":[]}"#)
            .with_header("x-request-id", "catalog-correlation"),
    ])
    .await;
    let mut base_url = server.base_url();
    base_url.set_path("/api");
    let provider = WireProvider::Ollama.build(base_url);
    let error = provider
        .model_info(ModelId::new("missing-installed-model").unwrap())
        .await
        .expect_err("a valid absent identity is a missing resource");
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert_eq!(error.context().operation, Operation::ModelInfo);
    assert_eq!(error.context().phase, ErrorPhase::Normalize);
    assert_eq!(error.http_status(), None);
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("catalog-correlation")
    );
    assert_eq!(
        server.requests().len(),
        1,
        "missing catalog entry cannot initiate show"
    );
    provider.shutdown().await.expect("provider shutdown");
}

#[tokio::test]
async fn openai_stream_fields_distinguish_absence_from_invalid_values() {
    for (event, parent, key, field) in [
        ("response.created", None, "type", "type"),
        (
            "response.created",
            Some("response"),
            "model",
            "response.model",
        ),
        ("response.output_text.delta", None, "delta", "delta"),
        ("response.refusal.delta", None, "delta", "delta"),
        (
            "response.reasoning_summary_text.delta",
            None,
            "delta",
            "delta",
        ),
        ("response.reasoning_text.delta", None, "delta", "delta"),
        (
            "response.output_item.added",
            Some("item"),
            "type",
            "item.type",
        ),
        (
            "response.output_item.done",
            Some("item"),
            "type",
            "item.type",
        ),
    ] {
        for value in [
            None,
            Some(Value::Null),
            Some(json!(7)),
            Some(json!(false)),
            Some(json!([])),
            Some(json!({})),
        ] {
            let mut payload = json!({"type":event,"fixture":"sensitive-field-body"});
            match event {
                "response.created" => payload["response"] = json!({"model":"wire-model"}),
                "response.output_item.added" | "response.output_item.done" => {
                    payload["item"] = json!({"type":"message","role":"assistant","content":[]});
                }
                _ => payload["delta"] = json!("fixture"),
            }
            let object = if let Some(parent) = parent {
                payload[parent].as_object_mut().unwrap()
            } else {
                payload.as_object_mut().unwrap()
            };
            object.remove(key);
            let expected = if let Some(value) = value {
                object.insert(key.to_owned(), value);
                ResponseViolation::InvalidField { field }
            } else {
                ResponseViolation::MissingField { field }
            };
            WireProvider::OpenAi
                .assert_invalid_stream_frame(
                    ScriptedResponse::sse(&format!(
                        "id: sensitive-field-event\ndata: {payload}\n\n"
                    )),
                    expected,
                    ErrorPhase::Normalize,
                )
                .await;
        }
    }
    for (event, field) in [
        ("response.created", "response"),
        ("response.output_item.added", "item"),
        ("response.output_item.done", "item"),
    ] {
        for value in [
            None,
            Some(Value::Null),
            Some(json!(7)),
            Some(json!(false)),
            Some(json!([])),
            Some(json!("invalid")),
        ] {
            let mut payload = json!({"type":event,"fixture":"sensitive-field-body"});
            let expected = if let Some(value) = value {
                payload[field] = value;
                ResponseViolation::InvalidField { field }
            } else {
                ResponseViolation::MissingField { field }
            };
            WireProvider::OpenAi
                .assert_invalid_stream_frame(
                    ScriptedResponse::sse(&format!(
                        "id: sensitive-field-event\ndata: {payload}\n\n"
                    )),
                    expected,
                    ErrorPhase::Normalize,
                )
                .await;
        }
    }
}

#[tokio::test]
async fn anthropic_stream_fields_distinguish_absence_from_invalid_values() {
    let message_start = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_wire\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"wire-model\",\"content\":[]}}\n\n";
    let text_prefix = format!(
        "{message_start}event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n"
    );
    for (event, parent, discriminator, key, field) in [
        (
            "error",
            "error",
            "overloaded_error",
            "message",
            "error.message",
        ),
        (
            "content_block_delta",
            "delta",
            "text_delta",
            "type",
            "delta.type",
        ),
        (
            "content_block_delta",
            "delta",
            "text_delta",
            "text",
            "delta.text",
        ),
        (
            "content_block_delta",
            "delta",
            "thinking_delta",
            "thinking",
            "delta.thinking",
        ),
        (
            "content_block_delta",
            "delta",
            "input_json_delta",
            "partial_json",
            "delta.partial_json",
        ),
        (
            "content_block_delta",
            "delta",
            "signature_delta",
            "signature",
            "delta.signature",
        ),
    ] {
        let (block, baseline) = match discriminator {
            "overloaded_error" => (None, json!({"type":discriminator,"message":"fixture"})),
            "text_delta" => (
                Some(json!({"type":"text","text":""})),
                json!({"type":discriminator,"text":"fixture"}),
            ),
            "thinking_delta" => (
                Some(json!({"type":"thinking","thinking":"","signature":""})),
                json!({"type":discriminator,"thinking":"fixture"}),
            ),
            "signature_delta" => (
                Some(json!({"type":"thinking","thinking":"","signature":""})),
                json!({"type":discriminator,"signature":"signature"}),
            ),
            "input_json_delta" => (
                Some(json!({"type":"tool_use","id":"call_wire","name":"lookup","input":{}})),
                json!({"type":discriminator,"partial_json":"{}"}),
            ),
            _ => unreachable!("fixture discriminator is explicit"),
        };
        let prefix = if let Some(block) = block {
            let start = json!({"type":"content_block_start","index":0,"content_block":block});
            format!("{message_start}event: content_block_start\ndata: {start}\n\n")
        } else {
            message_start.to_owned()
        };
        let mut values = vec![
            None,
            Some(Value::Null),
            Some(json!(7)),
            Some(json!(false)),
            Some(json!([])),
            Some(json!({})),
        ];
        if key == "signature" {
            values.extend([Some(json!("")), Some(json!(" \t"))]);
        }
        for value in values {
            let mut payload = json!({"type":event,"index":0,"fixture":"sensitive-field-body"});
            payload[parent] = baseline.clone();
            let object = payload[parent].as_object_mut().unwrap();
            object.remove(key);
            let expected = if let Some(value) = value {
                object.insert(key.to_owned(), value);
                ResponseViolation::InvalidField { field }
            } else {
                ResponseViolation::MissingField { field }
            };
            WireProvider::Anthropic
                .assert_invalid_stream_frame(
                    ScriptedResponse::sse(&format!(
                        "{prefix}id: sensitive-field-event\nevent: {event}\ndata: {payload}\n\n"
                    )),
                    expected,
                    ErrorPhase::Stream,
                )
                .await;
        }
    }
    for (event, field) in [("error", "error"), ("content_block_delta", "delta")] {
        let prefix = if event == "error" {
            message_start
        } else {
            text_prefix.as_str()
        };
        for value in [
            None,
            Some(Value::Null),
            Some(json!(7)),
            Some(json!(false)),
            Some(json!([])),
            Some(json!("invalid")),
        ] {
            let mut payload = json!({"type":event,"index":0,"fixture":"sensitive-field-body"});
            let expected = if let Some(value) = value {
                payload[field] = value;
                ResponseViolation::InvalidField { field }
            } else {
                ResponseViolation::MissingField { field }
            };
            WireProvider::Anthropic
                .assert_invalid_stream_frame(
                    ScriptedResponse::sse(&format!(
                        "{prefix}id: sensitive-field-event\nevent: {event}\ndata: {payload}\n\n"
                    )),
                    expected,
                    ErrorPhase::Stream,
                )
                .await;
        }
    }
    for event in [
        "content_block_start",
        "content_block_delta",
        "content_block_stop",
    ] {
        for value in [
            None,
            Some(Value::Null),
            Some(json!("0")),
            Some(json!(-1)),
            Some(json!(0.5)),
            Some(json!(false)),
        ] {
            let mut payload = json!({"type":event,"fixture":"sensitive-field-body"});
            let prefix = if event == "content_block_start" {
                payload["content_block"] = json!({"type":"text","text":""});
                message_start
            } else {
                if event == "content_block_delta" {
                    payload["delta"] = json!({"type":"text_delta","text":"fixture"});
                }
                text_prefix.as_str()
            };
            let expected = if let Some(value) = value {
                payload["index"] = value;
                ResponseViolation::InvalidField { field: "index" }
            } else {
                ResponseViolation::MissingField { field: "index" }
            };
            WireProvider::Anthropic
                .assert_invalid_stream_frame(
                    ScriptedResponse::sse(&format!(
                        "{prefix}id: sensitive-field-event\nevent: {event}\ndata: {payload}\n\n"
                    )),
                    expected,
                    ErrorPhase::Stream,
                )
                .await;
        }
    }
    for value in [None, Some(Value::Null)] {
        let mut payload = json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","name":"lookup","input":{}},"fixture":"sensitive-field-body"});
        let expected = if let Some(value) = value {
            payload["content_block"]["id"] = value;
            ResponseViolation::InvalidField {
                field: "content_block.id",
            }
        } else {
            ResponseViolation::MissingField {
                field: "content_block.id",
            }
        };
        WireProvider::Anthropic.assert_invalid_stream_frame(
            ScriptedResponse::sse(&format!("{message_start}id: sensitive-field-event\nevent: content_block_start\ndata: {payload}\n\n")),
            expected,
            ErrorPhase::Stream,
        ).await;
    }
}

#[tokio::test]
async fn ollama_present_invalid_stream_errors_are_never_missing_fields() {
    for value in [
        Value::Null,
        json!(7),
        json!(false),
        json!([]),
        json!({}),
        json!(""),
        json!(" \t"),
    ] {
        let payload = json!({"error":value,"fixture":"sensitive-field-body"});
        WireProvider::Ollama
            .assert_invalid_stream_frame(
                ScriptedResponse::chunks(
                    200,
                    "application/x-ndjson",
                    vec![format!("{payload}\n").into_bytes()],
                ),
                ResponseViolation::InvalidField { field: "error" },
                ErrorPhase::Normalize,
            )
            .await;
    }
}

#[tokio::test]
async fn stream_unknown_discriminators_and_malformed_json_keep_distinct_reasons() {
    for (payload, field) in [
        (
            json!({"type":"unknown-event","fixture":"sensitive-field-body"}),
            "type",
        ),
        (
            json!({"type":"response.output_item.added","item":{"type":"unknown-item"},"fixture":"sensitive-field-body"}),
            "item.type",
        ),
    ] {
        WireProvider::OpenAi
            .assert_invalid_stream_frame(
                ScriptedResponse::sse(&format!("id: sensitive-field-event\ndata: {payload}\n\n")),
                ResponseViolation::UnknownVariant { field },
                ErrorPhase::Normalize,
            )
            .await;
    }
    WireProvider::Anthropic.assert_invalid_stream_frame(
        ScriptedResponse::sse("id: sensitive-field-event\nevent: unknown-event\ndata: {\"fixture\":\"sensitive-field-body\"}\n\n"),
        ResponseViolation::UnknownVariant { field: "event" },
        ErrorPhase::Stream,
    ).await;
    WireProvider::Anthropic.assert_invalid_stream_frame(
        ScriptedResponse::sse(concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_wire\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"wire-model\",\"content\":[]}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "id: sensitive-field-event\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"unknown-delta\"},\"fixture\":\"sensitive-field-body\"}\n\n",
        )),
        ResponseViolation::UnknownVariant { field: "delta.type" },
        ErrorPhase::Stream,
    ).await;
    for provider in [
        WireProvider::OpenAi,
        WireProvider::Anthropic,
        WireProvider::Ollama,
    ] {
        let response = if matches!(provider, WireProvider::Ollama) {
            ScriptedResponse::chunks(
                200,
                "application/x-ndjson",
                vec![b"{sensitive-field-body\n".to_vec()],
            )
        } else {
            ScriptedResponse::sse(
                "id: sensitive-field-event\nevent: message_start\ndata: {sensitive-field-body\n\n",
            )
        };
        provider
            .assert_invalid_stream_frame(
                response,
                ResponseViolation::MalformedJson,
                ErrorPhase::Decode,
            )
            .await;
    }
}

#[tokio::test]
async fn valid_empty_stream_fragments_still_reach_authoritative_completion() {
    let openai_frames = [
        json!({"type":"response.created","response":{"model":"wire-model"}}),
        json!({"type":"response.output_text.delta","delta":""}),
        json!({"type":"response.reasoning_summary_text.delta","delta":""}),
        json!({"type":"response.output_text.delta","delta":"done"}),
        json!({"type":"response.completed","response":{"model":"wire-model","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}}),
    ].into_iter().map(|payload| format!("data: {payload}\n\n")).collect::<String>();
    let anthropic_frames = [
        ("message_start", json!({"type":"message_start","message":{"id":"msg_wire","type":"message","role":"assistant","model":"wire-model","content":[]}})),
        ("content_block_start", json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})),
        ("content_block_delta", json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":""}})),
        ("content_block_delta", json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}})),
        ("content_block_stop", json!({"type":"content_block_stop","index":0})),
        ("content_block_start", json!({"type":"content_block_start","index":1,"content_block":{"type":"thinking","thinking":"","signature":""}})),
        ("content_block_delta", json!({"type":"content_block_delta","index":1,"delta":{"type":"thinking_delta","thinking":""}})),
        ("content_block_delta", json!({"type":"content_block_delta","index":1,"delta":{"type":"signature_delta","signature":"native-signature"}})),
        ("content_block_stop", json!({"type":"content_block_stop","index":1})),
        ("content_block_start", json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"call_wire","name":"lookup","input":{}}})),
        ("content_block_delta", json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":""}})),
        ("content_block_delta", json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"value\":\"fixture\"}"}})),
        ("content_block_stop", json!({"type":"content_block_stop","index":2})),
        ("message_delta", json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}})),
        ("message_stop", json!({"type":"message_stop"})),
    ].into_iter().map(|(event, payload)| format!("event: {event}\ndata: {payload}\n\n")).collect::<String>();
    for (kind, frames) in [
        (WireProvider::OpenAi, openai_frames),
        (WireProvider::Anthropic, anthropic_frames),
    ] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::sse(&frames)]).await;
        let provider = kind.build(server.base_url());
        let model = kind.model(&provider, "wire-model");
        let mut stream = model
            .generate_stream(&WireRequest::user("fixture"))
            .await
            .expect("stream opens");
        let mut empty_text = 0;
        let mut empty_thinking = 0;
        let mut completed = None;
        while let Some(event) = stream.next().await {
            match event.expect("valid empty fragments must normalize") {
                GenerateEvent::ContentDelta { delta } if delta.is_empty() => empty_text += 1,
                GenerateEvent::ThinkingDelta { delta } if delta.is_empty() => empty_thinking += 1,
                GenerateEvent::Completed(response) => {
                    assert!(
                        completed.replace(response).is_none(),
                        "one authoritative completion"
                    );
                }
                _ => {}
            }
        }
        assert_eq!(empty_text, 1);
        assert_eq!(empty_thinking, 1);
        let response = completed.expect("valid stream completes");
        assert!(
            response
                .message
                .content()
                .iter()
                .any(|part| matches!(part, ContentPart::Text { text } if text == "done"))
        );
        if matches!(kind, WireProvider::Anthropic) {
            assert_eq!(response.finish_reason, Some(FinishReason::ToolCall));
            let [call] = response.message.function_calls() else {
                panic!("empty partial fragment must preserve the one complete call")
            };
            assert_eq!(call.id(), Some("call_wire"));
            assert_eq!(call.arguments(), &json!({"value":"fixture"}));
        } else {
            assert_eq!(response.finish_reason, Some(FinishReason::Stop));
        }
        assert!(stream.next().await.is_none());
        provider.shutdown().await.expect("provider shutdown");
    }
}

#[tokio::test]
async fn ollama_absent_error_field_is_an_ordinary_successful_stream() {
    let server = spawn_scripted_http_server(vec![ScriptedResponse::chunks(
        200,
        "application/x-ndjson",
        vec![b"{\"model\":\"wire-model\",\"message\":{\"role\":\"assistant\",\"content\":\"done\"},\"done\":true,\"done_reason\":\"stop\"}\n".to_vec()],
    )]).await;
    let mut base_url = server.base_url();
    base_url.set_path("/api");
    let provider = WireProvider::Ollama.build(base_url);
    let model = WireProvider::Ollama.model(&provider, "wire-model");
    let events = model
        .generate_stream(&WireRequest::user("fixture"))
        .await
        .expect("stream opens")
        .try_collect::<Vec<_>>()
        .await
        .expect("absence of an error envelope is valid");
    let Some(GenerateEvent::Completed(response)) = events.last() else {
        panic!("normal stream completes")
    };
    assert_eq!(response.finish_reason, Some(FinishReason::Stop));
    assert_eq!(
        response.message.content(),
        &[ContentPart::Text {
            text: "done".to_owned()
        }]
    );
    provider.shutdown().await.expect("provider shutdown");
}

#[tokio::test]
async fn openai_unknown_model_minimal_requests_reach_native_missing_model_errors() {
    let selected = "providers-kit-missing-model-wire-17";
    let rejection = json!({"error":{"type":"invalid_request_error","code":"model_not_found","param":"model","message":"sensitive-missing-model"}}).to_string();
    let event = json!({"type":"error","sequence_number":0,"code":"model_not_found","param":"model","message":"sensitive-missing-model"});
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::error(404, &rejection)
            .with_header("x-request-id", "sensitive-missing-unary"),
        ScriptedResponse::error(404, &rejection)
            .with_header("x-request-id", "sensitive-missing-opening"),
        ScriptedResponse::sse(&format!(
            "id: sensitive-missing-event\nevent: error\ndata: {event}\n\n"
        ))
        .with_header("x-request-id", "sensitive-missing-consumption"),
    ])
    .await;
    let provider = WireProvider::OpenAi.build(server.base_url());
    let model = WireProvider::OpenAi.model(&provider, selected);
    assert_eq!(model.model_id().as_str(), selected);
    let request = WireRequest::user("fixture");
    let unary = model
        .generate(&request)
        .await
        .expect_err("missing model is rejected by HTTP");
    let opening = match model.generate_stream(&request).await {
        Ok(_) => panic!("HTTP 404 must reject stream opening"),
        Err(error) => error,
    };
    let mut stream = model
        .generate_stream(&request)
        .await
        .expect("HTTP 200 stream opens");
    let in_band = stream
        .next()
        .await
        .expect("one native stream error")
        .expect_err("missing-model code is not successful completion");
    assert_eq!(
        in_band.native_details().event_id.as_deref(),
        Some("sensitive-missing-event")
    );
    assert!(stream.next().await.is_none());
    for (error, operation, status, correlation) in [
        (
            unary,
            Operation::Generate,
            Some(404),
            "sensitive-missing-unary",
        ),
        (
            opening,
            Operation::GenerateStream,
            Some(404),
            "sensitive-missing-opening",
        ),
        (
            in_band,
            Operation::GenerateStream,
            None,
            "sensitive-missing-consumption",
        ),
    ] {
        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert_eq!(error.context().operation, operation);
        assert_eq!(
            error.context().phase,
            if status.is_some() {
                ErrorPhase::ResponseHeaders
            } else {
                ErrorPhase::Stream
            }
        );
        assert_eq!(error.http_status(), status);
        assert_eq!(
            error.native_details().code.as_deref(),
            Some("model_not_found")
        );
        assert_eq!(error.native_details().parameter.as_deref(), Some("model"));
        assert_eq!(
            error.native_details().request_id.as_deref(),
            Some(correlation)
        );
        assert!(
            error
                .native_details()
                .body
                .as_deref()
                .is_some_and(|body| body.contains("sensitive-missing-model"))
        );
        assert!(!error.to_string().contains("sensitive-"));
        assert!(!format!("{error:?}").contains("sensitive-"));
    }
    let requests = server.requests();
    assert_eq!(requests.len(), 3, "no discovery or fallback requests");
    for (index, recorded) in requests.iter().enumerate() {
        assert_eq!(recorded.method, "POST");
        assert_eq!(recorded.path, "/responses");
        let body: Value = serde_json::from_slice(&recorded.body).expect("native request JSON");
        assert_eq!(body["model"], selected);
        for control in [
            "temperature",
            "top_p",
            "top_k",
            "max_output_tokens",
            "stop",
            "presence_penalty",
            "frequency_penalty",
            "seed",
            "reasoning",
            "tools",
            "tool_choice",
            "text",
        ] {
            assert!(
                body.get(control).is_none(),
                "minimal missing-model request must omit {control}"
            );
        }
        if index == 0 {
            assert!(body.get("stream").is_none());
        } else {
            assert_eq!(body["stream"], true);
        }
    }
    provider.shutdown().await.expect("provider shutdown");
}

#[tokio::test]
async fn resolution_openai_batch_reads_success_and_error_files_as_one_result() {
    for output_present in [false, true] {
        let mut replies = vec![ScriptedResponse::json(
            &json!({
                "id":"batch_remote", "status":"completed", "error_file_id":"file-errors",
                "output_file_id": output_present.then_some("file-output")
            })
            .to_string(),
        )];
        if output_present {
            replies.push(ScriptedResponse::json(r#"{"custom_id":"good","response":{"status_code":200,"request_id":"request-good","body":{"model":"exact-native-model","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}},"error":null}"#));
        }
        replies.push(ScriptedResponse::json(r#"{"custom_id":"bad","response":{"status_code":400,"request_id":"request-bad","body":{"error":{"type":"invalid_request_error","code":"invalid_file","message":"file-owned is unavailable","native_extra":"retained"}}},"error":null}"#));
        let (result, requests) = WireProvider::OpenAi
            .observe_batch_results(replies, "batch_remote", 4096)
            .await
            .expect("bounded batch results and provider shutdown");
        let rows = result.expect("all accepted remote rows remain returned data");
        assert_eq!(rows.len(), if output_present { 2 } else { 1 });
        let BatchResult::Failed { custom_id, error } = rows.last().unwrap() else {
            panic!("error-file row missing")
        };
        assert_eq!(custom_id, "bad");
        assert_eq!(error.kind(), ErrorKind::InvalidRequest);
        assert_eq!(error.native_details().code.as_deref(), Some("invalid_file"));
        assert_eq!(
            error.native_details().error_type.as_deref(),
            Some("invalid_request_error")
        );
        assert_eq!(error.native_details().reported_http_status, Some(400));
        assert_eq!(
            error.native_details().request_id.as_deref(),
            Some("request-bad")
        );
        assert!(
            error
                .native_details()
                .body
                .as_deref()
                .is_some_and(|body| body.contains("native_extra"))
        );
        assert_eq!(requests.len(), if output_present { 3 } else { 2 });
    }
}

#[tokio::test]
async fn openai_batch_valid_rows_enforce_one_aggregate_byte_budget() {
    let first = WireResponse::openai_batch_success_row();
    let second = WireResponse::openai_batch_error_row("sensitive-bad");
    let total = first.len() + second.len();
    let mut failures = Vec::new();
    for chunked in [false, true] {
        for limit in [total - 1, total] {
            assert!(first.len() <= limit && second.len() <= limit);
            let reply = if chunked {
                let split = second.len() / 2;
                ScriptedResponse::chunks(
                    200,
                    "application/json",
                    vec![
                        second.as_bytes()[..split].to_vec(),
                        second.as_bytes()[split..].to_vec(),
                    ],
                )
            } else {
                ScriptedResponse::json(&second)
            };
            let observed = WireProvider::OpenAi
                .observe_batch_results(
                    WireResponse::openai_batch_files(reply),
                    "batch_remote",
                    limit,
                )
                .await;
            let passed = match &observed {
                Ok((result, requests)) => {
                    let requests_match = requests
                        .iter()
                        .map(|r| (r.method.as_str(), r.path.as_str()))
                        .collect::<Vec<_>>()
                        == [
                            ("GET", "/batches/batch_remote"),
                            ("GET", "/files/file-one/content"),
                            ("GET", "/files/file-two/content"),
                        ];
                    let outcome_matches = if limit < total {
                        matches!(result, Err(error)
                            if error.kind() == ErrorKind::InvalidResponse(ResponseViolation::LimitExceeded { limit })
                            && error.context().operation == Operation::BatchResults
                            && error.context().phase == ErrorPhase::ReadBody
                            && error.context().row.is_none() && error.correlation_id().is_none()
                            && error.http_status().is_none()
                            && error.response_details() == Some(&ResponseDetails::Size { observed: total })
                            && error.native_details().request_id.as_deref() == Some("sensitive-error-file-request"))
                    } else {
                        matches!(result, Ok(rows) if matches!(rows.as_slice(),
                            [BatchResult::Succeeded { custom_id: good, output }, BatchResult::Failed { custom_id: bad, error }]
                            if good == "sensitive-good" && bad == "sensitive-bad"
                            && output.model.as_str() == "exact-native-model"
                            && output.finish_reason == Some(FinishReason::Stop)
                            && output.message.content() == [ContentPart::Text { text: "fixture answer".into() }]
                            && error.kind() == ErrorKind::InvalidRequest
                            && error.native_details().code.as_deref() == Some("invalid_file")
                            && error.native_details().error_type.as_deref() == Some("invalid_request_error")
                            && error.native_details().reported_http_status == Some(400)
                            && error.native_details().request_id.as_deref() == Some("sensitive-error-row-request")))
                    };
                    requests_match && outcome_matches
                }
                Err(_) => false,
            };
            let case = format!("chunked={chunked},limit={limit},total={total}");
            eprintln!(
                "REGRESSION_CASE {}",
                json!({"case":case,"passed":passed,"expected":if limit < total {"aggregate LimitExceeded with exact observed total"} else {"two exact distinct outcomes"},"actual":format!("{observed:?}")})
            );
            if !passed {
                failures.push(case);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "aggregate byte-budget cases failed: {failures:?}"
    );
}

#[tokio::test]
async fn openai_batch_second_file_integrity_preserves_exact_diagnostics() {
    let mut failures = Vec::new();
    for (name, reply, kind, phase, row, correlation) in [
        (
            "duplicate",
            ScriptedResponse::json(&WireResponse::openai_batch_error_row("sensitive-good")),
            ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation {
                rule: "batch result custom ids must be unique",
            }),
            ErrorPhase::Normalize,
            Some(1),
            Some("sensitive-good"),
        ),
        (
            "malformed",
            ScriptedResponse::json("\nnot-json\n"),
            ErrorKind::InvalidResponse(ResponseViolation::MalformedJson),
            ErrorPhase::Decode,
            Some(1),
            None,
        ),
        (
            "missing_outcome",
            ScriptedResponse::json(r#"{"custom_id":"sensitive-bad","response":null,"error":null}"#),
            ErrorKind::InvalidResponse(ResponseViolation::InvalidField { field: "response" }),
            ErrorPhase::Normalize,
            Some(1),
            Some("sensitive-bad"),
        ),
        (
            "download_404",
            ScriptedResponse::error(
                404,
                r#"{"error":{"type":"invalid_request_error","code":"not_found","message":"sensitive missing result file"}}"#,
            ),
            ErrorKind::NotFound,
            ErrorPhase::ResponseHeaders,
            None,
            None,
        ),
    ] {
        let observed = WireProvider::OpenAi
            .observe_batch_results(
                WireResponse::openai_batch_files(reply),
                "batch_remote",
                4096,
            )
            .await;
        let passed = match &observed {
            Ok((Err(error), requests)) => {
                error.kind() == kind
                    && error.context().operation == Operation::BatchResults
                    && error.context().phase == phase
                    && error.context().row == row
                    && error.correlation_id() == correlation
                    && error.http_status() == (name == "download_404").then_some(404)
                    && error.native_details().request_id.as_deref()
                        == Some("sensitive-error-file-request")
                    && (name != "malformed"
                        || matches!(error.response_details(), Some(ResponseDetails::Json { .. })))
                    && !format!("{error:?} {error}").contains("sensitive")
                    && requests
                        .iter()
                        .map(|r| (r.method.as_str(), r.path.as_str()))
                        .collect::<Vec<_>>()
                        == [
                            ("GET", "/batches/batch_remote"),
                            ("GET", "/files/file-one/content"),
                            ("GET", "/files/file-two/content"),
                        ]
            }
            _ => false,
        };
        eprintln!(
            "REGRESSION_CASE {}",
            json!({"case":name,"passed":passed,"expected":format!("{kind:?}/{phase:?}/row={row:?}/correlation={correlation:?}"),"actual":format!("{observed:?}")})
        );
        if !passed {
            failures.push(name);
        }
    }
    assert!(
        failures.is_empty(),
        "second-file integrity cases failed: {failures:?}"
    );
}

#[tokio::test]
async fn resolution_openai_job_preserves_every_distinct_native_failure() {
    let server =
        spawn_scripted_http_server(vec![ScriptedResponse::json(&json!({
        "id":"batch_remote","status":"failed","errors":{"data":[
            {"code":"invalid_file","message":"first-secret","param":"input_file_id","line":1},
            {"code":"invalid_request","message":"second-secret","param":"endpoint","line":2}
        ]}
    }).to_string())])
        .await;
    let provider = WireProvider::OpenAi.build(server.base_url());
    let job = provider.batch_status("batch_remote".into()).await.unwrap();
    provider.shutdown().await.unwrap();
    let BatchStatus::Failed { errors } = &job.status else {
        panic!("failed status expected")
    };
    assert_eq!(errors.len(), 2);
    assert_eq!(
        errors[1].native_details().message.as_deref(),
        Some("second-secret")
    );
    assert_eq!(
        errors[1].native_details().parameter.as_deref(),
        Some("endpoint")
    );
    assert!(!format!("{job:?}").contains("secret"));
}

#[tokio::test]
async fn resolution_anthropic_batch_preserves_inner_subtype_and_request_identity() {
    let rows = ["not_found_error", "invalid_request_error"].map(|kind| json!({
        "custom_id":kind,"result":{"type":"errored","error":{"type":"error","error":{"type":kind,"message":"file-owned is unavailable"},"request_id":"native-request"}}
    }).to_string()).join("\n");
    let results = spawn_scripted_http_server(vec![ScriptedResponse::json(&rows)]).await;
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json(&json!({"id":"msgbatch_remote","processing_status":"ended","results_url":results.base_url()}).to_string())]).await;
    let provider = WireProvider::Anthropic.build(server.base_url());
    let result = provider
        .batch_results("msgbatch_remote".into(), NonZeroUsize::new(4096).unwrap())
        .await;
    provider.shutdown().await.unwrap();
    let rows = result.expect("accepted row errors are returned data");
    for (row, expected) in rows
        .iter()
        .zip([ErrorKind::NotFound, ErrorKind::InvalidRequest])
    {
        let BatchResult::Failed { custom_id, error } = row else {
            panic!("failed row expected")
        };
        assert_eq!(error.kind(), expected);
        assert_eq!(
            error.native_details().error_type.as_deref(),
            Some(custom_id.as_str())
        );
        assert_eq!(error.native_details().event_type.as_deref(), Some("error"));
        assert_eq!(
            error.native_details().request_id.as_deref(),
            Some("native-request")
        );
    }
}

#[tokio::test]
async fn resolution_gemini_file_batch_decodes_wrapped_success_and_rpc_failure() {
    let success = json!({"key":"good","response":{"modelVersion":"gemini-native-version","candidates":[{"content":{"role":"model","parts":[{"text":"answer"}]},"finishReason":"STOP"}]}});
    let failure = json!({"key":"bad","error":{"code":7,"message":"files/owned denied","details":[{"@type":"type.googleapis.com/google.rpc.ResourceInfo","resourceType":"file","resourceName":"files/owned"}]}});
    let replies = vec![
        ScriptedResponse::json(
            r#"{"name":"batches/wire","metadata":{"state":"JOB_STATE_SUCCEEDED"},"response":{"responsesFile":"files/output"}}"#,
        ),
        ScriptedResponse::json(&format!("{success}\n{failure}\n")),
    ];
    let (result, _) = WireProvider::Gemini
        .observe_batch_results(replies, "batches/wire", 4096)
        .await
        .expect("bounded batch results and provider shutdown");
    let rows = result.expect("documented provider-file wrapper normalizes");
    assert!(
        matches!(&rows[0], BatchResult::Succeeded { custom_id, output } if custom_id == "good" && output.model.as_str() == "gemini-native-version")
    );
    let BatchResult::Failed { custom_id, error } = &rows[1] else {
        panic!("RPC error row missing")
    };
    assert_eq!(custom_id, "bad");
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.native_details().rpc_code, Some(7));
    assert_eq!(error.native_details().reported_http_status, None);
    assert!(
        matches!(&error.native_details().details[0], providers_kit::error::NativeDetail::Resource { name, .. } if name == "files/owned")
    );
}

#[tokio::test]
async fn gemini_file_batch_rejects_invalid_envelopes_with_row_evidence() {
    let native_response = WireResponse::gemini_batch_success_row()["response"].clone();
    let duplicate =
        json!({"key":"sensitive-good","error":{"code":7,"message":"sensitive row detail"}});
    let conflicting_metadata = json!({"metadata":{"custom_id":"sensitive-metadata"},"key":"sensitive-key","response":native_response});
    let conflicting_custom =
        json!({"key":"sensitive-key","custom_id":"sensitive-custom","response":native_response});
    let both = json!({"key":"sensitive-bad","response":native_response,"error":{"code":7,"message":"sensitive row detail"}});
    let neither = json!({"key":"sensitive-bad"});
    let nulls = json!({"key":"sensitive-bad","response":null,"error":null});
    let exactly_one = ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation {
        rule: "batch row must contain exactly one response or error",
    });
    let mut failures = Vec::new();
    for (name, second, kind, phase, correlation, identity) in [
        (
            "duplicate",
            duplicate.to_string(),
            ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation {
                rule: "batch result custom ids must be unique",
            }),
            ErrorPhase::Normalize,
            Some("sensitive-good"),
            None,
        ),
        (
            "conflicting_metadata",
            conflicting_metadata.to_string(),
            ErrorKind::InvalidResponse(ResponseViolation::IdentityMismatch { field: "custom_id" }),
            ErrorPhase::Normalize,
            None,
            Some(("sensitive-metadata", "sensitive-key")),
        ),
        (
            "conflicting_custom",
            conflicting_custom.to_string(),
            ErrorKind::InvalidResponse(ResponseViolation::IdentityMismatch { field: "custom_id" }),
            ErrorPhase::Normalize,
            None,
            Some(("sensitive-key", "sensitive-custom")),
        ),
        (
            "both",
            both.to_string(),
            exactly_one,
            ErrorPhase::Normalize,
            Some("sensitive-bad"),
            None,
        ),
        (
            "neither",
            neither.to_string(),
            exactly_one,
            ErrorPhase::Normalize,
            Some("sensitive-bad"),
            None,
        ),
        (
            "nulls",
            nulls.to_string(),
            exactly_one,
            ErrorPhase::Normalize,
            Some("sensitive-bad"),
            None,
        ),
        (
            "malformed",
            "{\"key\":".to_owned(),
            ErrorKind::InvalidResponse(ResponseViolation::MalformedJson),
            ErrorPhase::Decode,
            None,
            None,
        ),
    ] {
        let observed = WireProvider::Gemini
            .observe_batch_results(
                WireResponse::gemini_batch_files(&second),
                "batches/wire",
                4096,
            )
            .await;
        let passed = match &observed {
            Ok((Err(error), requests)) => {
                let details_match = if let Some((expected, actual)) = identity {
                    error.response_details()
                        == Some(&ResponseDetails::Identity {
                            expected: expected.to_owned(),
                            actual: actual.to_owned(),
                        })
                } else if name == "malformed" {
                    matches!(error.response_details(), Some(ResponseDetails::Json {
                        category: JsonErrorCategory::Eof, line: 1, column,
                    }) if *column > 0)
                } else {
                    true
                };
                error.kind() == kind
                    && error.context().operation == Operation::BatchResults
                    && error.context().phase == phase
                    && error.context().row == Some(1)
                    && error.correlation_id() == correlation
                    && error.http_status().is_none()
                    && error.remote_failure().is_none()
                    && error.native_details().request_id.as_deref()
                        == Some("sensitive-download-request")
                    && error
                        .native_details()
                        .body
                        .as_deref()
                        .is_some_and(|body| body.contains(&second))
                    && !format!("{error:?} {error}").contains("sensitive")
                    && details_match
                    && requests
                        .iter()
                        .map(|r| (r.method.as_str(), r.path.as_str()))
                        .collect::<Vec<_>>()
                        == [
                            ("GET", "/v1beta/batches/wire"),
                            ("GET", "/download/v1beta/files/output:download?alt=media"),
                        ]
            }
            _ => false,
        };
        eprintln!(
            "REGRESSION_CASE {}",
            json!({"case":name,"passed":passed,"expected":format!("{kind:?}/{phase:?}/row=1/correlation={correlation:?}"),"actual":format!("{observed:?}")})
        );
        if !passed {
            failures.push(name);
        }
    }
    assert!(
        failures.is_empty(),
        "Gemini malformed envelope cases failed: {failures:?}"
    );
}

#[tokio::test]
async fn resolution_google_error_details_preserve_repeated_machine_facts() {
    let body = json!({"error":{"code":400,"status":"INVALID_ARGUMENT","message":"private-body","details":[
        {"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"UNKNOWN_REASON","domain":"other.example"},
        {"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"API_KEY_INVALID","domain":"googleapis.com"},
        {"@type":"type.googleapis.com/google.rpc.ResourceInfo","resourceType":"file","resourceName":"files/native","owner":"native-owner"},
        {"@type":"type.googleapis.com/google.rpc.BadRequest","fieldViolations":[{"field":"input","description":"bad input"},{"field":"model","description":"bad model"}]},
        {"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"1.000000002s"},
        {"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"subject":"quota-subject","description":"quota-description"}]},
        {"@type":"type.googleapis.com/google.rpc.PreconditionFailure","violations":[{"type":"TOS","subject":"tos-subject","description":"accept terms"}]}
    ]}});
    let server =
        spawn_scripted_http_server(vec![ScriptedResponse::error(400, &body.to_string())]).await;
    let provider = WireProvider::Gemini.build(server.base_url());
    let error = WireProvider::Gemini
        .model(&provider, "models/gemini-wire")
        .generate(&WireRequest::user("probe"))
        .await
        .unwrap_err();
    provider.shutdown().await.unwrap();
    assert_eq!(error.kind(), ErrorKind::Authentication);
    assert_eq!(
        error.classification_basis(),
        providers_kit::error::ClassificationBasis::NativeReason
    );
    assert_eq!(error.http_status(), Some(400));
    assert_eq!(error.native_details().reported_http_status, Some(400));
    assert_eq!(error.native_details().rpc_code, None);
    assert_eq!(
        error.native_details().code,
        None,
        "reason is not a service-code field"
    );
    assert_eq!(error.native_details().details.len(), 8);
    assert_eq!(
        error.native_details().body.as_deref(),
        Some(body.to_string().as_str())
    );
}

#[tokio::test]
async fn resolution_stream_error_envelopes_keep_remote_basis_and_sequence() {
    for payload in [
        json!({"type":"error","sequence_number":7,"code":null,"message":"private-native","param":null}),
        json!({"type":"error","sequence_number":8,"error":{"code":null,"type":"not_found_error","message":"files/owned missing"}}),
    ] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::sse(&format!(
            "event: error\nid: native-event\ndata: {payload}\n\n"
        ))])
        .await;
        let provider = WireProvider::OpenAi.build(server.base_url());
        let model = WireProvider::OpenAi.model(&provider, "gpt-4.1");
        let mut stream = model
            .generate_stream(&WireRequest::user("probe"))
            .await
            .unwrap();
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(stream.next().await.is_none());
        provider.shutdown().await.unwrap();
        assert!(
            error.remote_failure().is_some(),
            "accepted-stream native error remains a remote failure"
        );
        assert_eq!(error.http_status(), None);
        assert_eq!(
            error.native_details().sequence_number,
            payload["sequence_number"].as_u64()
        );
        assert_eq!(error.native_details().event_type.as_deref(), Some("error"));
        assert_eq!(
            error.native_details().event_id.as_deref(),
            Some("native-event")
        );
        assert_eq!(
            error.kind(),
            if payload.get("error").is_some() {
                ErrorKind::NotFound
            } else {
                ErrorKind::ProviderRejected
            }
        );
    }
}

#[tokio::test]
async fn resolution_stream_schema_observations_are_retained_and_redacted() {
    use providers_kit::error::{JsonValueKind, ResponseDetails};
    let malformed = WireProvider::OpenAi
        .stream_failure(ScriptedResponse::sse("event: error\ndata: {\n\n"))
        .await;
    assert!(matches!(
        malformed.response_details(),
        Some(ResponseDetails::Json { line: 1, .. })
    ));
    let wrong_type = WireProvider::OpenAi
        .stream_failure(ScriptedResponse::sse("data: {\"type\":37}\n\n"))
        .await;
    assert_eq!(
        wrong_type.response_details(),
        Some(&ResponseDetails::FieldType {
            expected: JsonValueKind::String,
            actual: JsonValueKind::Number
        })
    );
    let unknown = WireProvider::OpenAi
        .stream_failure(ScriptedResponse::sse(
            "data: {\"type\":\"private_new_event\"}\n\n",
        ))
        .await;
    assert_eq!(
        unknown.response_details(),
        Some(&ResponseDetails::Discriminator {
            actual: "private_new_event".into()
        })
    );
    assert!(!format!("{unknown:?}").contains("private_new_event"));
    let index = WireProvider::Anthropic.stream_failure(ScriptedResponse::sse("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":3,\"delta\":{\"type\":\"text_delta\",\"text\":\"x\"}}\n\n")).await;
    assert_eq!(
        index.response_details(),
        Some(&ResponseDetails::Index {
            index: 3,
            available: 0
        })
    );
    let gemini = WireProvider::Gemini.stream_failure(ScriptedResponse::sse(&format!("data: {}\n\ndata: {}\n\n",
        json!({"modelVersion":"gemini-first","candidates":[{"content":{"role":"model","parts":[{"text":"first"}]}}]}),
        json!({"modelVersion":"gemini-second","candidates":[{"content":{"role":"model","parts":[{"text":"second"}]},"finishReason":"STOP"}]})))).await;
    assert_eq!(
        gemini.response_details(),
        Some(&ResponseDetails::Identity {
            expected: "gemini-first".into(),
            actual: "gemini-second".into()
        })
    );
    let ollama = WireProvider::Ollama.stream_failure(ScriptedResponse::json("{\"model\":\"different-native-model\",\"message\":{\"role\":\"assistant\",\"content\":\"x\"},\"done\":true}\n")).await;
    assert_eq!(
        ollama.response_details(),
        Some(&ResponseDetails::Identity {
            expected: "wire-model".into(),
            actual: "different-native-model".into()
        })
    );
}

#[tokio::test]
async fn resolution_openai_empty_second_file_is_valid_at_zero_remaining_budget() {
    let row = r#"{"custom_id":"one","error":{"code":null,"type":"invalid_request_error","message":"native rejected row"}}"#;
    let replies = vec![
        ScriptedResponse::json(
            r#"{"id":"batch_remote","status":"completed","output_file_id":"file-one","error_file_id":"file-two"}"#,
        ),
        ScriptedResponse::json(row),
        ScriptedResponse::json(""),
    ];
    let (result, requests) = WireProvider::OpenAi
        .observe_batch_results(replies, "batch_remote", row.len())
        .await
        .expect("bounded batch results and provider shutdown");
    let rows = result.expect("empty second file requires zero further bytes");
    assert_eq!(rows.len(), 1);
    assert!(
        matches!(&rows[0], BatchResult::Failed { error, .. } if error.kind() == ErrorKind::InvalidRequest && error.native_details().code.is_none())
    );
    assert_eq!(requests.len(), 3);
}

#[tokio::test]
async fn resolution_gemini_success_status_cannot_be_a_failed_remote_row() {
    for native in [
        json!({"code":0,"message":"contradictory success"}),
        json!({"code":3,"status":"OK","message":"contradictory success"}),
    ] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::json(&json!({
            "name":"batches/wire","metadata":{"state":"JOB_STATE_SUCCEEDED"},
            "response":{"inlinedResponses":{"inlinedResponses":[{"metadata":{"custom_id":"row"},"error":native}]}}
        }).to_string())]).await;
        let provider = WireProvider::Gemini.build(server.base_url());
        let result = provider
            .batch_results("batches/wire".into(), NonZeroUsize::new(4096).unwrap())
            .await;
        provider.shutdown().await.unwrap();
        let error = result.expect_err("native success cannot establish remote failure");
        assert!(matches!(error.kind(), ErrorKind::InvalidResponse(_)));
        assert_eq!(error.context().row, Some(0));
        assert_eq!(error.correlation_id(), Some("row"));
    }
}

#[tokio::test]
async fn resolution_openai_failed_job_does_not_require_optional_native_diagnostics() {
    for native in [
        json!({"object":"list","data":null}),
        json!({"data":[{"code":"invalid_file","line":7}]}),
    ] {
        let expected = native["data"].as_array().map_or(0, Vec::len);
        let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
            &json!({"id":"batch_remote","status":"failed","errors":native}).to_string(),
        )])
        .await;
        let provider = WireProvider::OpenAi.build(server.base_url());
        let result = provider.batch_status("batch_remote".into()).await;
        provider.shutdown().await.unwrap();
        let job =
            result.expect("optional failed-job diagnostics cannot invalidate native job status");
        let BatchStatus::Failed { errors } = job.status else {
            panic!("failed native status expected")
        };
        assert_eq!(errors.len(), expected);
        if let Some(error) = errors.first() {
            assert_eq!(error.native_details().code.as_deref(), Some("invalid_file"));
            assert_eq!(error.native_details().message, None);
            assert!(
                error
                    .native_details()
                    .body
                    .as_deref()
                    .is_some_and(|body| body.contains("\"line\":7"))
            );
        }
    }
}

#[tokio::test]
async fn resolution_anthropic_malformed_row_retains_decoded_caller_correlation() {
    let results =
        spawn_scripted_http_server(vec![ScriptedResponse::json(r#"{"custom_id":"owned-row"}"#)])
            .await;
    let server = spawn_scripted_http_server(vec![ScriptedResponse::json(&json!({"id":"msgbatch_remote","processing_status":"ended","results_url":results.base_url()}).to_string())]).await;
    let provider = WireProvider::Anthropic.build(server.base_url());
    let error = provider
        .batch_results("msgbatch_remote".into(), NonZeroUsize::new(4096).unwrap())
        .await
        .unwrap_err();
    provider.shutdown().await.unwrap();
    assert!(matches!(error.kind(), ErrorKind::InvalidResponse(_)));
    assert_eq!(error.context().row, Some(0));
    assert_eq!(error.correlation_id(), Some("owned-row"));
    assert!(
        error
            .native_details()
            .details
            .iter()
            .all(|detail| !matches!(detail, providers_kit::error::NativeDetail::Resource { .. }))
    );
}

#[tokio::test]
async fn resolution_openai_accepted_failed_response_remains_batch_row_data() {
    let rows = [
        json!({"custom_id":"bad","response":{"status_code":200,"request_id":"native-accepted","body":{"model":"gpt-native","status":"failed","error":{"code":"server_error","message":"private-native-failure"}}},"error":null}),
        json!({"custom_id":"good","response":{"status_code":200,"body":{"model":"gpt-native","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}},"error":null}),
    ];
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(
            r#"{"id":"batch_remote","status":"completed","output_file_id":"file-output"}"#,
        ),
        ScriptedResponse::json(&format!("{}\n{}", rows[0], rows[1])),
    ])
    .await;
    let provider = WireProvider::OpenAi.build(server.base_url());
    let result = provider
        .batch_results("batch_remote".into(), NonZeroUsize::new(4096).unwrap())
        .await;
    provider.shutdown().await.unwrap();
    let rows = result.expect("accepted failed response must not erase neighboring successful row");
    assert_eq!(rows.len(), 2);
    assert!(matches!(&rows[1], BatchResult::Succeeded { custom_id, .. } if custom_id == "good"));
    let BatchResult::Failed { error, .. } = &rows[0] else {
        panic!("failed response row expected")
    };
    assert_eq!(error.kind(), ErrorKind::Internal);
    assert_eq!(error.native_details().reported_http_status, Some(200));
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("native-accepted")
    );
    assert_eq!(
        error.classification_basis(),
        providers_kit::error::ClassificationBasis::NativeCode
    );
}

#[tokio::test]
async fn resolution_hosted_field_shape_observations_survive_private_cursor_validation() {
    use providers_kit::error::{JsonValueKind, ResponseDetails};
    for (url, actual) in [
        (json!(42), JsonValueKind::Number),
        (Value::Null, JsonValueKind::Null),
    ] {
        let server = spawn_scripted_http_server(vec![ScriptedResponse::json(
            &json!({
                "model":"gpt-4.1", "status":"completed", "output":[{
                    "type":"web_search_call", "id":"owned-search", "status":"completed",
                    "action":{"type":"find_in_page", "url":url, "pattern":"IANA"}
                }]
            })
            .to_string(),
        )])
        .await;
        let provider = WireProvider::OpenAi.build(server.base_url());
        let error = WireProvider::OpenAi
            .model(&provider, "gpt-4.1")
            .generate(&WireRequest::user("Find IANA."))
            .await
            .unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::InvalidResponse(_)));
        assert_eq!(
            error.response_details(),
            Some(&ResponseDetails::FieldType {
                expected: JsonValueKind::String,
                actual
            })
        );
        provider.shutdown().await.unwrap();
    }
}
