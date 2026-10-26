use std::num::NonZeroU32;
use std::time::Duration;

use futures_util::StreamExt;
use provider_sdk::error::{
    ErrorKind, ErrorPhase, JsonValueKind, Operation, ResponseDetails, ResponseViolation,
};
use provider_sdk::generation::{
    ContentPart, FinishReason, GenerateEvent, GenerateResponse, GenerationConfig, ProviderToolSpec,
    ReasoningConfig, ToolChoice, Usage,
};
use provider_sdk::hosted::{HostedTool, HostedToolOutput, HostedToolStatus};
use provider_sdk::{ModelBackend, ProviderBackend, SdkError};
use serde_json::{Value, json};

use crate::support::scripted_http::{ScriptedResponse, spawn_scripted_http_server};
use crate::{WireProvider, WireRequest};

struct GeminiStreamCase {
    frames: Vec<Value>,
}

impl GeminiStreamCase {
    async fn unary(native: &Value) -> Result<GenerateResponse, SdkError> {
        let server =
            spawn_scripted_http_server(vec![ScriptedResponse::json(&native.to_string())]).await;
        let provider = WireProvider::Gemini.build(server.base_url());
        let model = WireProvider::Gemini.model(&provider, "models/gemini-wire");
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            model.generate(&WireRequest::user("fixture")),
        )
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        assert!(
            matches!(shutdown, Ok(Ok(()))),
            "operation: {result:?}; shutdown: {shutdown:?}"
        );
        result.expect("generation deadline")
    }

    fn text(text: &str) -> Value {
        json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":text}]}}]})
    }

    fn terminal(parts: Option<Value>) -> Value {
        let mut frame = json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":{"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2,"totalTokenCount":6}});
        if let Some(parts) = parts {
            frame["candidates"][0]["content"]["parts"] = parts;
        }
        frame
    }

    fn reply(&self) -> ScriptedResponse {
        ScriptedResponse::sse(
            &self
                .frames
                .iter()
                .map(|frame| format!("data: {frame}\n\n"))
                .collect::<String>(),
        )
    }

    async fn observe(self) -> Vec<Result<GenerateEvent, SdkError>> {
        Self::observe_reply(self.reply()).await
    }

    async fn observe_reply(reply: ScriptedResponse) -> Vec<Result<GenerateEvent, SdkError>> {
        let server = spawn_scripted_http_server(vec![reply]).await;
        let provider = WireProvider::Gemini.build(server.base_url());
        let model = WireProvider::Gemini.model(&provider, "models/gemini-wire");
        let observed = tokio::time::timeout(Duration::from_secs(10), async {
            let mut stream = model.generate_stream(&WireRequest::user("fixture")).await?;
            let events = stream.by_ref().collect::<Vec<_>>().await;
            let eof = stream.next().await.is_none();
            Ok::<_, SdkError>((events, eof))
        })
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        assert!(
            matches!(shutdown, Ok(Ok(()))),
            "shutdown: {shutdown:?}; operation: {observed:?}"
        );
        assert_eq!(server.requests().len(), 1);
        let (events, eof) = observed.expect("stream deadline").expect("stream opens");
        assert!(eof, "EOF must remain terminal");
        events
    }

    async fn assert_error(self, reason: ResponseViolation) -> SdkError {
        Self::observed_error(self.observe().await, reason)
    }

    fn observed_error(
        events: Vec<Result<GenerateEvent, SdkError>>,
        reason: ResponseViolation,
    ) -> SdkError {
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Ok(GenerateEvent::Completed(_))))
        );
        assert_eq!(events.iter().filter(|event| event.is_err()).count(), 1);
        let Some(Err(error)) = events.into_iter().last() else {
            panic!("invalid stream must end in exactly one error")
        };
        assert_eq!(error.kind(), ErrorKind::InvalidResponse(reason));
        assert_eq!(error.context().operation, Operation::GenerateStream);
        error
    }

    fn completed(
        expected_model: &str,
        events: Vec<Result<GenerateEvent, SdkError>>,
    ) -> GenerateResponse {
        let mut events = events
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("all stream frames normalize");
        assert!(
            matches!(events.first(), Some(GenerateEvent::Started { model }) if model.as_str() == expected_model)
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, GenerateEvent::Started { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, GenerateEvent::Completed(_)))
                .count(),
            1
        );
        let Some(GenerateEvent::Completed(response)) = events.pop() else {
            panic!("completion must be the last event")
        };
        let deltas = events
            .iter()
            .filter_map(|event| match event {
                GenerateEvent::ContentDelta { delta } => Some(delta.as_str()),
                _ => None,
            })
            .collect::<String>();
        let text = response
            .message
            .content()
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(
            deltas, text,
            "completion must contain each delta exactly once"
        );
        assert_eq!(response.model.as_str(), expected_model);
        *response
    }
}

#[tokio::test]
async fn reaudit_gemini_optional_role_preserves_success_unary() {
    for role in [
        None,
        Some(Value::Null),
        Some(json!("")),
        Some(json!("model")),
    ] {
        let mut native = GeminiStreamCase::terminal(Some(json!([{ "text": "actual output" }])));
        let content = native["candidates"][0]["content"].as_object_mut().unwrap();
        if let Some(role) = role {
            content.insert("role".to_owned(), role);
        } else {
            content.remove("role");
        }
        let response = GeminiStreamCase::unary(&native)
            .await
            .expect("optional native role");
        assert_eq!(response.finish_reason, Some(FinishReason::Stop));
        assert_eq!(
            response.message.content(),
            &[ContentPart::Text {
                text: "actual output".to_owned()
            }]
        );
    }
}

#[tokio::test]
async fn reaudit_gemini_optional_role_preserves_success_stream() {
    for role in [
        None,
        Some(Value::Null),
        Some(json!("")),
        Some(json!("model")),
    ] {
        let mut native = GeminiStreamCase::terminal(Some(json!([{ "text": "actual output" }])));
        let content = native["candidates"][0]["content"].as_object_mut().unwrap();
        if let Some(role) = role {
            content.insert("role".to_owned(), role);
        } else {
            content.remove("role");
        }
        let response = GeminiStreamCase::completed(
            "models/gemini-wire",
            GeminiStreamCase {
                frames: vec![native],
            }
            .observe()
            .await,
        );
        assert_eq!(response.finish_reason, Some(FinishReason::Stop));
        assert_eq!(
            response.message.content(),
            &[ContentPart::Text {
                text: "actual output".to_owned()
            }]
        );
    }
}

#[tokio::test]
async fn reaudit_gemini_blocked_prompt_retains_usage_tail() {
    let response = GeminiStreamCase::completed("models/gemini-wire", GeminiStreamCase { frames: vec![
        json!({"modelVersion":"gemini-native-version","promptFeedback":{"blockReason":"SAFETY"}}),
        json!({"modelVersion":"gemini-native-version","usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":0,"totalTokenCount":7}}),
    ] }.observe().await);
    assert_eq!(response.finish_reason, Some(FinishReason::ContentFilter));
    assert!(response.message.content().is_empty());
    assert_eq!(
        response.usage,
        Usage {
            input_tokens: Some(7),
            output_tokens: Some(0),
            total_tokens: Some(7),
            ..Usage::default()
        }
    );
}

#[tokio::test]
async fn reaudit_gemini_prompt_block_rejects_candidate_contradictions() {
    let blocked =
        json!({"modelVersion":"gemini-native-version","promptFeedback":{"blockReason":"SAFETY"}});
    let completed = GeminiStreamCase::terminal(Some(json!([{ "text": "candidate output" }])));
    for frames in [
        vec![blocked.clone(), completed.clone()],
        vec![completed, blocked],
    ] {
        GeminiStreamCase { frames }
            .assert_error(ResponseViolation::ProtocolViolation {
                rule: "prompt block must not coexist with candidate output",
            })
            .await;
    }
}

#[tokio::test]
async fn reaudit_gemini_filtered_absence_retains_prior_parts_stream() {
    for content in [None, Some(Value::Null), Some(json!({}))] {
        let mut terminal = json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"finishReason":"SAFETY"}]});
        if let Some(content) = content {
            terminal["candidates"][0]["content"] = content;
        }
        let response = GeminiStreamCase::completed(
            "models/gemini-wire",
            GeminiStreamCase {
                frames: vec![GeminiStreamCase::text("actual partial output"), terminal],
            }
            .observe()
            .await,
        );
        assert_eq!(response.finish_reason, Some(FinishReason::ContentFilter));
        assert_eq!(
            response.message.content(),
            &[ContentPart::Text {
                text: "actual partial output".to_owned()
            }]
        );
    }
}

#[tokio::test]
async fn reaudit_gemini_blocked_prompt_preserves_filtered_outcome() {
    let native =
        json!({"modelVersion":"gemini-native-version","promptFeedback":{"blockReason":"SAFETY"}});
    let unary = GeminiStreamCase::unary(&native)
        .await
        .expect("blocked prompt is a filtered completion");
    assert_eq!(unary.finish_reason, Some(FinishReason::ContentFilter));
    assert!(unary.message.content().is_empty());
    let streamed = GeminiStreamCase::completed(
        "models/gemini-wire",
        GeminiStreamCase {
            frames: vec![native],
        }
        .observe()
        .await,
    );
    assert_eq!(streamed.finish_reason, Some(FinishReason::ContentFilter));
    assert!(streamed.message.content().is_empty());
}

#[tokio::test]
async fn reaudit_gemini_filtered_candidate_allows_absent_content_unary() {
    let native = json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"finishReason":"SAFETY"}]});
    let response = GeminiStreamCase::unary(&native)
        .await
        .expect("blocked output may omit content");
    assert_eq!(response.finish_reason, Some(FinishReason::ContentFilter));
    assert!(response.message.content().is_empty());
}

#[tokio::test]
async fn reaudit_gemini_filtered_candidate_allows_absent_content_stream() {
    let native = json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"finishReason":"SAFETY"}]});
    let response = GeminiStreamCase::completed(
        "models/gemini-wire",
        GeminiStreamCase {
            frames: vec![native],
        }
        .observe()
        .await,
    );
    assert_eq!(response.finish_reason, Some(FinishReason::ContentFilter));
    assert!(response.message.content().is_empty());
}

#[tokio::test]
async fn reaudit_gemini_filtered_candidate_allows_null_content_unary() {
    let native = json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":null,"finishReason":"SAFETY"}]});
    let response = GeminiStreamCase::unary(&native)
        .await
        .expect("ProtoJSON null content is unset in a filtered response");
    assert_eq!(response.finish_reason, Some(FinishReason::ContentFilter));
    assert!(response.message.content().is_empty());
}

#[tokio::test]
async fn reaudit_gemini_filtered_candidate_allows_null_content_stream() {
    let native = json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":null,"finishReason":"SAFETY"}]});
    let response = GeminiStreamCase::completed(
        "models/gemini-wire",
        GeminiStreamCase {
            frames: vec![native],
        }
        .observe()
        .await,
    );
    assert_eq!(response.finish_reason, Some(FinishReason::ContentFilter));
    assert!(response.message.content().is_empty());
}

#[tokio::test]
async fn reaudit_gemini_filtered_candidate_allows_empty_content_unary() {
    let native = json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":{},"finishReason":"SAFETY"}]});
    let response = GeminiStreamCase::unary(&native)
        .await
        .expect("filtered content may have no parts and omit its optional role");
    assert_eq!(response.finish_reason, Some(FinishReason::ContentFilter));
    assert!(response.message.content().is_empty());
}

#[tokio::test]
async fn reaudit_gemini_filtered_candidate_allows_empty_content_stream() {
    let native = json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":{},"finishReason":"SAFETY"}]});
    let response = GeminiStreamCase::completed(
        "models/gemini-wire",
        GeminiStreamCase {
            frames: vec![native],
        }
        .observe()
        .await,
    );
    assert_eq!(response.finish_reason, Some(FinishReason::ContentFilter));
    assert!(response.message.content().is_empty());
}

#[tokio::test]
async fn reaudit_gemini_filtered_absence_does_not_accept_malformed_success() {
    for native in [
        json!({"modelVersion":"gemini-native-version","promptFeedback":{"blockReason":"UNKNOWN_NEW_REASON"}}),
        json!({"modelVersion":"gemini-native-version","promptFeedback":{"blockReason":"SAFETY"},"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"impossible"}]},"finishReason":"STOP"}]}),
        json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"finishReason":"STOP"}]}),
        json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":null,"finishReason":"STOP"}]}),
        json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":{},"finishReason":"STOP"}]}),
        json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":"malformed","finishReason":"SAFETY"}]}),
        json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":{"role":"model","parts":"malformed"},"finishReason":"SAFETY"}]}),
        json!({"modelVersion":"gemini-native-version","candidates":[{"index":0,"content":{"role":"model","parts":[null]},"finishReason":"SAFETY"}]}),
    ] {
        let error = GeminiStreamCase::unary(&native)
            .await
            .expect_err("malformed response must fail normalization");
        assert!(
            matches!(error.kind(), ErrorKind::InvalidResponse(_)),
            "malformed response: {native}; error: {error:?}"
        );
        assert_eq!(error.context().operation, Operation::Generate);
        let events = GeminiStreamCase {
            frames: vec![native.clone()],
        }
        .observe()
        .await;
        assert_eq!(
            events.iter().filter(|event| event.is_err()).count(),
            1,
            "malformed response: {native}; events: {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Ok(GenerateEvent::Completed(_))))
        );
        let Some(Err(error)) = events.last() else {
            panic!("malformed stream must end in an error: {native}; events: {events:?}");
        };
        assert!(
            matches!(error.kind(), ErrorKind::InvalidResponse(_)),
            "malformed response: {native}; error: {error:?}"
        );
        assert_eq!(error.context().operation, Operation::GenerateStream);
    }
}

#[tokio::test]
async fn native_stop_before_later_parts_waits_for_stream_eof() {
    let thought_code = "print('thought-phase declaration')";
    let visible_code = "print('visible-phase declaration')";
    let mut thought = GeminiStreamCase::text("");
    thought["candidates"][0]["content"]["parts"] = json!([{
        "thought": true,
        "executableCode": {"language": "PYTHON", "code": thought_code}
    }]);
    let early_stop = GeminiStreamCase::terminal(Some(json!([{
        "executableCode": {"language": "PYTHON", "code": visible_code}
    }])));
    let mut final_stop = GeminiStreamCase::terminal(Some(json!([{"text": "marker"}])));
    final_stop["usageMetadata"] = json!({
        "promptTokenCount": 7, "candidatesTokenCount": 11, "totalTokenCount": 18
    });
    final_stop["candidates"][0]["groundingMetadata"] = json!({
        "retrievalQueries": ["fixture lookup"],
        "groundingChunks": [{"retrievedContext": {"text": "stream-marker"}}]
    });
    let mut frames = vec![
        thought,
        early_stop,
        GeminiStreamCase::text("Answer: "),
        GeminiStreamCase::text("stream-"),
        final_stop,
    ];
    for frame in &mut frames {
        frame["responseId"] = json!("owned-stream-response");
    }
    let response = GeminiStreamCase::completed(
        "models/gemini-wire",
        GeminiStreamCase { frames }.observe().await,
    );
    assert_eq!(
        response.message.content(),
        [ContentPart::Text {
            text: "Answer: stream-marker".into()
        }]
    );
    assert_eq!(response.finish_reason, Some(FinishReason::Stop));
    assert_eq!(
        response.usage,
        Usage {
            input_tokens: Some(7),
            output_tokens: Some(11),
            total_tokens: Some(18),
            ..Usage::default()
        }
    );
    let mut code = Vec::new();
    let mut searches = 0;
    for output in &response.hosted_tool_outputs {
        match output {
            HostedToolOutput::CodeExecution {
                status,
                code: sources,
                results,
                ..
            } => {
                assert!(
                    status.is_none(),
                    "declarations do not establish execution status"
                );
                assert!(
                    results.is_empty(),
                    "the native stream returned no execution result"
                );
                for source in sources {
                    assert_eq!(source.language.as_deref(), Some("PYTHON"));
                    code.push(source.code.as_str());
                }
            }
            HostedToolOutput::FileSearch {
                status,
                queries,
                results,
                citations,
            } => {
                searches += 1;
                assert!(status.is_none());
                assert_eq!(queries, &["fixture lookup"]);
                assert!(matches!(results, Some(results) if results.is_empty()));
                assert!(
                    citations.is_empty(),
                    "identity-free grounding must not invent citations"
                );
            }
            other => panic!("unexpected normalized activity: {other:?}"),
        }
    }
    assert_eq!(code, [thought_code, visible_code]);
    assert_eq!(
        searches, 1,
        "grounding after the early STOP must be retained"
    );
}

#[tokio::test]
async fn native_stop_before_trailing_error_never_publishes_completion() {
    let events = GeminiStreamCase {
        frames: vec![
            GeminiStreamCase::terminal(Some(json!([{"text": "provisional answer"}]))),
            json!({"error": {
                "code": 503,
                "status": "UNAVAILABLE",
                "message": "synthetic trailing native failure"
            }}),
        ],
    }
    .observe()
    .await;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Ok(GenerateEvent::Started { .. })))
            .count(),
        1
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(GenerateEvent::Completed(_))))
    );
    assert_eq!(events.iter().filter(|event| event.is_err()).count(), 1);
    let Some(Err(error)) = events.last() else {
        panic!("trailing native failure must terminate the stream")
    };
    assert_eq!(error.kind(), ErrorKind::ServiceUnavailable);
    assert_eq!(error.context().operation, Operation::GenerateStream);
    assert_eq!(error.context().phase, ErrorPhase::Stream);
    assert_eq!(
        error.native_details().native_status.as_deref(),
        Some("UNAVAILABLE")
    );
    assert_eq!(error.native_details().reported_http_status, Some(503));
    assert_eq!(
        error.native_details().message.as_deref(),
        Some("synthetic trailing native failure")
    );
}

#[tokio::test]
async fn native_stop_boundary_requires_finish_after_later_candidate_text() {
    GeminiStreamCase {
        frames: vec![
            GeminiStreamCase::terminal(Some(json!([{"text": "finished phase"}]))),
            GeminiStreamCase::text("unterminated next phase"),
        ],
    }
    .assert_error(ResponseViolation::UnexpectedEnd)
    .await;
}

#[tokio::test]
async fn native_stop_boundary_retains_usage_only_tail() {
    for (native_finish, expected_finish) in [
        ("STOP", FinishReason::Stop),
        ("MAX_TOKENS", FinishReason::Length),
    ] {
        let mut terminal = GeminiStreamCase::terminal(Some(json!([{"text": "observed answer"}])));
        terminal["candidates"][0]["finishReason"] = json!(native_finish);
        let response = GeminiStreamCase::completed(
            "models/gemini-wire",
            GeminiStreamCase {
                frames: vec![
                    terminal,
                    json!({
                        "modelVersion": "gemini-native-version",
                        "usageMetadata": {
                            "promptTokenCount": 9,
                            "candidatesTokenCount": 13,
                            "totalTokenCount": 22
                        }
                    }),
                ],
            }
            .observe()
            .await,
        );
        assert_eq!(response.finish_reason, Some(expected_finish));
        assert_eq!(
            response.message.content(),
            [ContentPart::Text {
                text: "observed answer".into()
            }]
        );
        assert_eq!(
            response.usage,
            Usage {
                input_tokens: Some(9),
                output_tokens: Some(13),
                total_tokens: Some(22),
                ..Usage::default()
            }
        );
    }
}

#[tokio::test]
async fn native_stop_boundary_cannot_hide_unknown_earlier_finish() {
    let mut unknown = GeminiStreamCase::terminal(Some(json!([{"text": "partial"}])));
    unknown["candidates"][0]["finishReason"] = json!("FUTURE_FINISH");
    GeminiStreamCase {
        frames: vec![unknown, GeminiStreamCase::terminal(Some(json!([])))],
    }
    .assert_error(ResponseViolation::UnknownVariant {
        field: "finishReason",
    })
    .await;
}

#[tokio::test]
async fn native_stop_boundary_final_length_remains_unsuccessful() {
    let mut limited = GeminiStreamCase::terminal(Some(json!([{"text": "truncated"}])));
    limited["candidates"][0]["finishReason"] = json!("MAX_TOKENS");
    let response = GeminiStreamCase::completed(
        "models/gemini-wire",
        GeminiStreamCase {
            frames: vec![
                GeminiStreamCase::terminal(Some(json!([{"text": "first phase "}]))),
                limited,
            ],
        }
        .observe()
        .await,
    );
    assert_eq!(response.finish_reason, Some(FinishReason::Length));
    assert_eq!(
        response.message.content(),
        [ContentPart::Text {
            text: "first phase truncated".into()
        }]
    );
}

#[tokio::test]
async fn native_stop_boundary_cannot_resume_candidate_after_length() {
    let mut limited = GeminiStreamCase::terminal(Some(json!([{"text": "truncated"}])));
    limited["candidates"][0]["finishReason"] = json!("MAX_TOKENS");
    let error = GeminiStreamCase {
        frames: vec![limited, GeminiStreamCase::terminal(Some(json!([])))],
    }
    .assert_error(ResponseViolation::ProtocolViolation {
        rule: "Gemini stream continued after an unsuccessful finish reason",
    })
    .await;
    assert_eq!(error.context().phase, ErrorPhase::Stream);
}

#[tokio::test]
async fn native_stop_boundary_cannot_hide_later_identity_failures() {
    for (pointer, value, expected) in [
        (
            "/modelVersion",
            json!("different-native-version"),
            ResponseViolation::IdentityMismatch {
                field: "modelVersion",
            },
        ),
        (
            "/candidates/0/index",
            json!(1),
            ResponseViolation::InvalidField {
                field: "candidates.index",
            },
        ),
        (
            "/candidates/0/content/role",
            json!("user"),
            ResponseViolation::InvalidField {
                field: "content.role",
            },
        ),
    ] {
        let mut invalid = GeminiStreamCase::terminal(Some(json!([])));
        *invalid.pointer_mut(pointer).expect("fixture field") = value;
        GeminiStreamCase {
            frames: vec![
                GeminiStreamCase::terminal(Some(json!([{"text": "provisional"}]))),
                invalid,
            ],
        }
        .assert_error(expected)
        .await;
    }
}

#[tokio::test]
async fn native_stop_before_complete_malformed_frame_fails_before_eof_completion() {
    let terminal = GeminiStreamCase::terminal(Some(json!([{"text": "provisional"}])));
    let events = GeminiStreamCase::observe_reply(ScriptedResponse::sse(&format!(
        "data: {terminal}\n\ndata: {{malformed}}\n\n"
    )))
    .await;
    let error = GeminiStreamCase::observed_error(events, ResponseViolation::MalformedJson);
    assert_eq!(error.context().phase, ErrorPhase::Decode);
}

#[tokio::test]
async fn native_stop_cannot_hide_incomplete_sse_data_at_eof() {
    let terminal = GeminiStreamCase::terminal(Some(json!([{"text": "provisional"}])));
    let tail = GeminiStreamCase::text("unframed tail");
    for suffix in [
        format!("data: {tail}"),
        format!("data: {tail}\n"),
        format!("data: {tail}\r\n"),
        "data: {malformed\n".into(),
    ] {
        let events = GeminiStreamCase::observe_reply(ScriptedResponse::sse(&format!(
            "data: {terminal}\n\n{suffix}"
        )))
        .await;
        let error = GeminiStreamCase::observed_error(events, ResponseViolation::UnexpectedEnd);
        assert_eq!(error.context().phase, ErrorPhase::Stream);
    }
}

#[tokio::test]
async fn eof_does_not_supply_a_missing_terminal_sse_blank_line() {
    let terminal = GeminiStreamCase::terminal(Some(json!([{"text": "unterminated event"}])));
    let events =
        GeminiStreamCase::observe_reply(ScriptedResponse::sse(&format!("data: {terminal}\n")))
            .await;
    let error = GeminiStreamCase::observed_error(events, ResponseViolation::UnexpectedEnd);
    assert_eq!(error.context().phase, ErrorPhase::Stream);
}

#[tokio::test]
async fn native_stop_cannot_hide_invalid_utf8_at_eof() {
    let terminal = GeminiStreamCase::terminal(Some(json!([{"text": "provisional"}])));
    for tail in [vec![0xff], vec![0xe2, 0x82]] {
        let events = GeminiStreamCase::observe_reply(ScriptedResponse::chunks(
            200,
            "text/event-stream",
            vec![format!("data: {terminal}\n\n").into_bytes(), tail],
        ))
        .await;
        let error = GeminiStreamCase::observed_error(events, ResponseViolation::InvalidUtf8);
        assert_eq!(error.context().phase, ErrorPhase::Decode);
    }
}

#[tokio::test]
async fn clean_eof_drains_final_frame_events_before_completion() {
    let first = GeminiStreamCase::text("fixture ");
    let terminal = GeminiStreamCase::terminal(Some(json!([{"text": "answer"}])));
    let body = format!("data: {first}\n\ndata: {terminal}\n\n");
    let events = GeminiStreamCase::observe_reply(ScriptedResponse::chunks(
        200,
        "text/event-stream",
        body.bytes().map(|byte| vec![byte]).collect(),
    ))
    .await;
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                Ok(GenerateEvent::ContentDelta { delta }) => Some(delta.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        ["fixture ", "answer"]
    );
    let response = GeminiStreamCase::completed("models/gemini-wire", events);
    assert_eq!(response.finish_reason, Some(FinishReason::Stop));
    assert_eq!(
        response.message.content(),
        [ContentPart::Text {
            text: "fixture answer".into()
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
async fn clean_eof_after_sse_comment_does_not_invent_an_event() {
    let terminal = GeminiStreamCase::terminal(Some(json!([{"text": "observed answer"}])));
    for comment in [": keepalive", ": keepalive\n"] {
        let events = GeminiStreamCase::observe_reply(ScriptedResponse::sse(&format!(
            "data: {terminal}\n\n{comment}"
        )))
        .await;
        let response = GeminiStreamCase::completed("models/gemini-wire", events);
        assert_eq!(response.finish_reason, Some(FinishReason::Stop));
        assert_eq!(
            response.message.content(),
            [ContentPart::Text {
                text: "observed answer".into()
            }]
        );
    }
}

#[tokio::test]
async fn shared_eof_ollama_final_ndjson_line_preserves_completion() {
    let payload = json!({
        "model": "ollama-wire",
        "message": {"role": "assistant", "content": "final answer"},
        "done": true,
        "done_reason": "stop"
    });
    let server = spawn_scripted_http_server(vec![ScriptedResponse::chunks(
        200,
        "application/x-ndjson",
        vec![payload.to_string().into_bytes()],
    )])
    .await;
    let mut base_url = server.base_url();
    base_url.set_path("/api");
    let provider = WireProvider::Ollama.build(base_url);
    let model = WireProvider::Ollama.model(&provider, "ollama-wire");
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        let mut stream = model.generate_stream(&WireRequest::user("fixture")).await?;
        let events = stream.by_ref().collect::<Vec<_>>().await;
        let eof = stream.next().await.is_none();
        Ok::<_, SdkError>((events, eof))
    })
    .await;
    let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    assert!(
        matches!(shutdown, Ok(Ok(()))),
        "shutdown: {shutdown:?}; operation: {observed:?}"
    );
    assert_eq!(server.requests().len(), 1);
    let (events, eof) = observed.expect("stream deadline").expect("stream opens");
    assert!(eof);
    let response = GeminiStreamCase::completed("ollama-wire", events);
    assert_eq!(response.finish_reason, Some(FinishReason::Stop));
    assert_eq!(
        response.message.content(),
        [ContentPart::Text {
            text: "final answer".into()
        }]
    );
}

#[tokio::test]
async fn file_search_thinking_budget_zero_is_the_only_stream_request_delta() {
    let prompt = "Use File Search to read the strict-test fixture document in the configured store. \
                  What is its strict-test fixture marker? Do not guess. Return exactly one \
                  answer line, Answer: <the complete stored marker>. Put any citations on \
                  a separate line. Do not write or execute code.";
    let mut terminal = GeminiStreamCase::terminal(None);
    terminal["candidates"][0]["groundingMetadata"] = json!({
        "groundingChunks": [{"retrievedContext": {"text": "fixture-marker"}}]
    });
    let fixture = GeminiStreamCase {
        frames: vec![GeminiStreamCase::text("Answer: fixture-marker"), terminal],
    };
    let server = spawn_scripted_http_server(vec![fixture.reply(), fixture.reply()]).await;
    let provider = WireProvider::Gemini.build(server.base_url());
    let model = WireProvider::Gemini.model(&provider, "models/gemini-2.5-flash");
    let baseline = WireRequest::user(prompt)
        .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::FileSearch {
            store_ids: vec!["fileSearchStores/fixture".into()],
        })])
        .unwrap()
        .with_generation(
            GenerationConfig::default()
                .with_max_output_tokens(NonZeroU32::new(4_096).unwrap())
                .with_tool_choice(ToolChoice::Auto),
        )
        .unwrap();
    let zero_budget = baseline.clone().with_reasoning(ReasoningConfig {
        max_reasoning_tokens: Some(0),
        ..ReasoningConfig::default()
    });
    let mut observations = Vec::new();
    for request in [&baseline, &zero_budget] {
        observations.push(
            tokio::time::timeout(Duration::from_secs(10), async {
                let mut stream = model.generate_stream(request).await?;
                let events = stream.by_ref().collect::<Vec<_>>().await;
                Ok::<_, SdkError>((events, stream.next().await.is_none()))
            })
            .await,
        );
    }
    let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    assert!(
        matches!(shutdown, Ok(Ok(()))),
        "shutdown={shutdown:?}; operations={observations:?}"
    );
    for observed in observations {
        let (events, eof) = observed.expect("stream deadline").expect("stream opens");
        assert!(eof, "EOF must remain terminal");
        let response = GeminiStreamCase::completed("models/gemini-2.5-flash", events);
        assert_eq!(response.finish_reason, Some(FinishReason::Stop));
        assert_eq!(
            response.message.content(),
            &[ContentPart::Text {
                text: "Answer: fixture-marker".into()
            }]
        );
        assert!(matches!(
            response.hosted_tool_outputs.as_slice(),
            [HostedToolOutput::FileSearch { status: None, .. }]
        ));
    }
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(request.method, "POST");
        assert_eq!(
            request.path,
            "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse"
        );
    }
    let baseline_body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let mut zero_budget_body: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(
        baseline_body["contents"],
        json!([{"role": "user", "parts": [{"text": prompt}]}])
    );
    assert_eq!(
        baseline_body["tools"],
        json!([{"fileSearch": {"file_search_store_names": ["fileSearchStores/fixture"]}}])
    );
    assert_eq!(
        baseline_body["generationConfig"],
        json!({"maxOutputTokens": 4096})
    );
    assert!(baseline_body.get("toolConfig").is_none());
    let thinking = zero_budget_body["generationConfig"]
        .as_object_mut()
        .unwrap()
        .remove("thinkingConfig")
        .expect("explicit zero budget must be rendered");
    assert_eq!(thinking, json!({"thinkingBudget": 0}));
    assert_eq!(
        zero_budget_body, baseline_body,
        "removing only thinkingConfig must restore the complete baseline request body"
    );
}

#[tokio::test]
async fn url_failure_stream_preserves_failure_but_rejects_dangling_grounding() {
    let url = "https://httpbin.org/status/404?strict_live=stream-fixture";
    for dangling in [true, false] {
        let mut terminal = GeminiStreamCase::terminal(None);
        terminal["candidates"][0]["urlContextMetadata"] = json!({"urlMetadata":[{
            "retrievedUrl":url,"urlRetrievalStatus":"URL_RETRIEVAL_STATUS_ERROR"
        }]});
        if dangling {
            terminal["candidates"][0]["groundingMetadata"] = json!({"groundingSupports":[{
                "segment":{"startIndex":0,"endIndex":16,"text":"RETRIEVAL_FAILED"},"groundingChunkIndices":[0]
            }]});
        }
        let fixture = GeminiStreamCase {
            frames: vec![
                GeminiStreamCase::text("RETRIEVAL_"),
                GeminiStreamCase::text("FAILED"),
                terminal,
            ],
        };
        let server = spawn_scripted_http_server(vec![fixture.reply()]).await;
        let provider = WireProvider::Gemini.build(server.base_url());
        let model = WireProvider::Gemini.model(&provider, "models/gemini-2.5-flash");
        let request = WireRequest::user("Retrieve the configured unavailable URL.")
            .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::UrlContext)])
            .unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(10), async {
            let mut stream = model.generate_stream(&request).await?;
            let events = stream.by_ref().collect::<Vec<_>>().await;
            Ok::<_, SdkError>((events, stream.next().await.is_none()))
        })
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        assert!(
            matches!(shutdown, Ok(Ok(()))),
            "shutdown={shutdown:?}; operation={observed:?}"
        );
        let (events, eof) = observed.unwrap().unwrap();
        assert!(eof);
        assert_eq!(server.requests().len(), 1);
        if dangling {
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, Ok(GenerateEvent::Completed(_))))
            );
            assert_eq!(events.iter().filter(|event| event.is_err()).count(), 1);
            let Some(Err(error)) = events.last() else {
                panic!("dangling reference must terminate with an error")
            };
            assert_eq!(
                error.kind(),
                ErrorKind::InvalidResponse(ResponseViolation::InvalidField {
                    field: "groundingChunkIndices"
                })
            );
            assert_eq!(
                error.response_details(),
                Some(&ResponseDetails::Index {
                    index: 0,
                    available: 0
                })
            );
        } else {
            let response = GeminiStreamCase::completed("models/gemini-2.5-flash", events);
            assert_eq!(response.finish_reason, Some(FinishReason::Stop));
            assert_eq!(
                response.message.content(),
                &[ContentPart::Text {
                    text: "RETRIEVAL_FAILED".into()
                }]
            );
            let [
                HostedToolOutput::UrlContext {
                    retrieved_url,
                    status: Some(HostedToolStatus::Failed { error }),
                    citations,
                    ..
                },
            ] = response.hosted_tool_outputs.as_slice()
            else {
                panic!("native URL failure must survive completion")
            };
            assert_eq!(retrieved_url.as_deref(), Some(url));
            assert_eq!(
                error.native_details().outcome.as_deref(),
                Some("URL_RETRIEVAL_STATUS_ERROR")
            );
            assert!(citations.is_empty());
        }
    }
}

#[tokio::test]
async fn terminal_parts_omitted_empty_null_or_additional_preserve_exact_text() {
    for (parts, expected) in [
        (None, "fixture answer"),
        (Some(json!([])), "fixture answer"),
        (Some(Value::Null), "fixture answer"),
        (Some(json!([{"text":"!"}])), "fixture answer!"),
    ] {
        let response = GeminiStreamCase::completed(
            "models/gemini-wire",
            GeminiStreamCase {
                frames: vec![
                    GeminiStreamCase::text("fixture "),
                    GeminiStreamCase::text("answer"),
                    GeminiStreamCase::terminal(parts),
                ],
            }
            .observe()
            .await,
        );
        assert_eq!(
            response.message.content(),
            [ContentPart::Text {
                text: expected.into()
            }]
        );
        assert_eq!(response.finish_reason, Some(FinishReason::Stop));
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
}

#[tokio::test]
async fn empty_filtered_parts_are_unset_in_unary_and_streaming() {
    for parts in [None, Some(json!([])), Some(Value::Null)] {
        let mut native = GeminiStreamCase::terminal(parts);
        native["candidates"][0]["finishReason"] = json!("SAFETY");
        let server =
            spawn_scripted_http_server(vec![ScriptedResponse::json(&native.to_string())]).await;
        let provider = WireProvider::Gemini.build(server.base_url());
        let model = WireProvider::Gemini.model(&provider, "models/gemini-wire");
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            model.generate(&WireRequest::user("fixture")),
        )
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        assert!(
            matches!(shutdown, Ok(Ok(()))),
            "shutdown: {shutdown:?}; operation: {result:?}"
        );
        let response = result
            .expect("generation deadline")
            .expect("unset repeated parts remain an empty filtered response");
        assert_eq!(response.finish_reason, Some(FinishReason::ContentFilter));
        assert!(response.message.content().is_empty());
        assert_eq!(server.requests().len(), 1);
        let streamed = GeminiStreamCase::completed(
            "models/gemini-wire",
            GeminiStreamCase {
                frames: vec![native],
            }
            .observe()
            .await,
        );
        assert_eq!(streamed.finish_reason, Some(FinishReason::ContentFilter));
        assert!(streamed.message.content().is_empty());
    }
}

#[tokio::test]
async fn role_only_terminal_retains_prior_hosted_metadata_and_results() {
    let mut first = GeminiStreamCase::text("fixture");
    first["candidates"][0]["content"]["parts"] = json!([
        {"executableCode":{"id":"code_one","language":"PYTHON","code":"print(1)"}},
        {"codeExecutionResult":{"id":"code_one","outcome":"OUTCOME_OK","output":"1\n"}},
        {"text":"fixture"}
    ]);
    first["candidates"][0]["groundingMetadata"] = json!({"webSearchQueries":["fixture"],"groundingChunks":[{"web":{"uri":"https://example.com/native","title":"Native"}}],"groundingSupports":[{"segment":{"startIndex":0,"endIndex":7,"text":"fixture"},"groundingChunkIndices":[0]}]});
    first["candidates"][0]["urlContextMetadata"] = json!({"urlMetadata":[{"retrievedUrl":"https://owned.invalid/native","urlRetrievalStatus":"URL_RETRIEVAL_STATUS_ERROR"}]});
    let response = GeminiStreamCase::completed(
        "models/gemini-wire",
        GeminiStreamCase {
            frames: vec![first, GeminiStreamCase::terminal(None)],
        }
        .observe()
        .await,
    );
    assert_eq!(response.hosted_tool_outputs.len(), 3);
    assert!(response.hosted_tool_outputs.iter().any(|output| matches!(output, HostedToolOutput::CodeExecution { status: None, code, results, .. } if code.len() == 1 && results.len() == 1 && results[0].stdout.as_deref() == Some("1\n") && results[0].status == Some(HostedToolStatus::Completed))));
    assert!(response.hosted_tool_outputs.iter().any(|output| matches!(output, HostedToolOutput::WebSearch { status: None, results, citations, .. } if results.len() == 1 && citations.len() == 1 && results[0].url == "https://example.com/native")));
    assert!(response.hosted_tool_outputs.iter().any(|output| matches!(output, HostedToolOutput::UrlContext { requested_url: None, retrieved_url: Some(url), status: Some(HostedToolStatus::Failed { error }), citations } if url == "https://owned.invalid/native" && error.native_details().outcome.as_deref() == Some("URL_RETRIEVAL_STATUS_ERROR") && citations.is_empty())));
}

#[tokio::test]
async fn role_only_terminal_replays_exact_signed_function_parts() {
    for role in [None, Some(""), Some("model")] {
        let parts = json!([
            {"text":"thinking","thought":true,"thoughtSignature":"signed-thought"},
            {"text":"checking "},
            {"functionCall":{"id":"call_one","name":"lookup_weather","args":{"city":"Chicago"}},"thoughtSignature":"signed-call"}
        ]);
        let mut first = GeminiStreamCase::text("");
        first["candidates"][0]["content"]["parts"] = parts.clone();
        let mut terminal = GeminiStreamCase::terminal(None);
        for frame in [&mut first, &mut terminal] {
            let content = frame["candidates"][0]["content"].as_object_mut().unwrap();
            match role {
                Some(role) => {
                    content.insert("role".into(), json!(role));
                }
                None => {
                    content.remove("role");
                }
            }
        }
        let fixture = GeminiStreamCase {
            frames: vec![first, terminal],
        };
        let server = spawn_scripted_http_server(vec![
            fixture.reply(),
            ScriptedResponse::json(
                &GeminiStreamCase::terminal(Some(json!([{"text":"done"}]))).to_string(),
            ),
        ])
        .await;
        let provider = WireProvider::Gemini.build(server.base_url());
        let model = WireProvider::Gemini.model(&provider, "models/gemini-wire");
        let observed = tokio::time::timeout(Duration::from_secs(10), async {
            let mut stream = model.generate_stream(&WireRequest::user("fixture")).await?;
            let events = stream.by_ref().collect::<Vec<_>>().await;
            let eof = stream.next().await.is_none();
            drop(stream);
            let events = events.into_iter().collect::<Result<Vec<_>, _>>()?;
            let response = events.iter().find_map(|event| match event {
                GenerateEvent::Completed(response) => Some(response),
                _ => None,
            });
            let followup = if let Some((response, call)) = response.and_then(|response| {
                response
                    .message
                    .function_calls()
                    .first()
                    .map(|call| (response, call))
            }) {
                Some(
                    model
                        .generate(&WireRequest::function_output(
                            response.message.clone(),
                            call,
                            vec![ContentPart::Json {
                                value: json!({"temperature_f":72}),
                            }],
                        ))
                        .await?,
                )
            } else {
                None
            };
            Ok::<_, SdkError>((events, eof, followup))
        })
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        assert!(
            matches!(shutdown, Ok(Ok(()))),
            "shutdown: {shutdown:?}; operation: {observed:?}"
        );
        let (events, eof, followup) = observed
            .expect("stream and replay deadline")
            .expect("unchanged streamed assistant must replay");
        assert!(eof, "stream stays at EOF");
        let response =
            GeminiStreamCase::completed("models/gemini-wire", events.into_iter().map(Ok).collect());
        assert_eq!(response.finish_reason, Some(FinishReason::ToolCall));
        assert_eq!(response.message.function_calls().len(), 1);
        assert_eq!(
            followup
                .expect("a correlated function response must be sent")
                .finish_reason,
            Some(FinishReason::Stop)
        );
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        let replay: Value = serde_json::from_slice(&requests[1].body).expect("request JSON");
        assert_eq!(replay["contents"][0]["parts"], parts);
        assert_eq!(
            replay["contents"][1]["parts"][0]["functionResponse"]["id"],
            "call_one"
        );
    }
}

#[tokio::test]
async fn malformed_terminal_parts_and_identity_cannot_be_repaired_into_success() {
    for parts in [json!("invalid"), json!({}), json!(37), json!([null])] {
        let error = GeminiStreamCase {
            frames: vec![
                GeminiStreamCase::text("fixture"),
                GeminiStreamCase::terminal(Some(parts)),
            ],
        }
        .assert_error(ResponseViolation::InvalidField { field: "response" })
        .await;
        assert_eq!(error.context().phase, ErrorPhase::Decode);
    }
    for (pointer, value, expected) in [
        (
            "/candidates/0/content/role",
            json!("user"),
            ResponseViolation::InvalidField {
                field: "content.role",
            },
        ),
        (
            "/candidates/0/index",
            json!(1),
            ResponseViolation::InvalidField {
                field: "candidates.index",
            },
        ),
        (
            "/candidates/0/finishReason",
            json!("FUTURE_FINISH"),
            ResponseViolation::UnknownVariant {
                field: "finishReason",
            },
        ),
        (
            "/modelVersion",
            json!("changed-model-version"),
            ResponseViolation::IdentityMismatch {
                field: "modelVersion",
            },
        ),
    ] {
        let mut terminal = GeminiStreamCase::terminal(None);
        *terminal.pointer_mut(pointer).expect("fixture field") = value;
        GeminiStreamCase {
            frames: vec![GeminiStreamCase::text("fixture"), terminal],
        }
        .assert_error(expected)
        .await;
    }
}

#[tokio::test]
async fn earlier_non_model_role_cannot_be_overwritten_by_valid_terminal() {
    let mut first = GeminiStreamCase::text("fixture");
    first["candidates"][0]["content"]["role"] = json!("user");
    GeminiStreamCase {
        frames: vec![first, GeminiStreamCase::terminal(Some(json!([])))],
    }
    .assert_error(ResponseViolation::InvalidField {
        field: "content.role",
    })
    .await;
}

#[tokio::test]
async fn earlier_nonzero_candidate_index_cannot_be_overwritten_by_valid_terminal() {
    let mut first = GeminiStreamCase::text("fixture");
    first["candidates"][0]["index"] = json!(1);
    GeminiStreamCase {
        frames: vec![first, GeminiStreamCase::terminal(Some(json!([])))],
    }
    .assert_error(ResponseViolation::InvalidField {
        field: "candidates.index",
    })
    .await;
}

#[tokio::test]
async fn earlier_multiple_candidates_cannot_be_discarded_by_valid_terminal() {
    let mut first = GeminiStreamCase::text("fixture");
    let second = first["candidates"][0].clone();
    first["candidates"]
        .as_array_mut()
        .expect("fixture candidates")
        .push(second);
    GeminiStreamCase {
        frames: vec![first, GeminiStreamCase::terminal(Some(json!([])))],
    }
    .assert_error(ResponseViolation::ProtocolViolation {
        rule: "response must contain exactly one candidate",
    })
    .await;
}

#[tokio::test]
async fn successful_terminal_content_reports_absence_separately_from_present_null() {
    // A successful STOP terminal requires content; the filtered-completion
    // exception does not permit missing content in this successful response.
    for (content, expected, actual) in [
        (
            None,
            ResponseViolation::MissingField {
                field: "candidates.content",
            },
            JsonValueKind::Absent,
        ),
        (
            Some(Value::Null),
            ResponseViolation::InvalidField {
                field: "candidates.content",
            },
            JsonValueKind::Null,
        ),
    ] {
        let mut terminal = GeminiStreamCase::terminal(None);
        let candidate = terminal["candidates"][0]
            .as_object_mut()
            .expect("fixture candidate");
        if let Some(content) = content {
            candidate.insert("content".into(), content);
        } else {
            candidate.remove("content");
        }
        let error = GeminiStreamCase {
            frames: vec![GeminiStreamCase::text("fixture"), terminal],
        }
        .assert_error(expected)
        .await;
        assert_eq!(
            error.response_details(),
            Some(&ResponseDetails::FieldType {
                expected: JsonValueKind::Object,
                actual
            })
        );
    }
}

#[tokio::test]
async fn empty_success_and_retained_part_overflow_still_fail() {
    GeminiStreamCase {
        frames: vec![GeminiStreamCase::terminal(None)],
    }
    .assert_error(ResponseViolation::ProtocolViolation {
        rule: "nonempty_generation",
    })
    .await;
    let mut first = GeminiStreamCase::text("");
    first["candidates"][0]["content"]["parts"] = Value::Array(vec![json!({"text":"x"}); 4_097]);
    let overflow = GeminiStreamCase {
        frames: vec![first, GeminiStreamCase::terminal(None)],
    }
    .observe()
    .await;
    assert!(
        matches!(overflow.last(), Some(Err(error)) if error.kind() == ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation { rule: "normalized stream state exceeded its item budget" }))
    );
    assert!(
        !overflow
            .iter()
            .any(|event| matches!(event, Ok(GenerateEvent::Completed(_))))
    );
}

#[tokio::test]
async fn native_stop_boundary_function_calls_preserve_unsuccessful_native_finish() {
    for (native_finish, expected_finish) in [
        ("STOP", FinishReason::ToolCall),
        ("MAX_TOKENS", FinishReason::Length),
    ] {
        let mut terminal = GeminiStreamCase::terminal(Some(json!([{
            "functionCall": {"id": "call_one", "name": "lookup_weather", "args": {"city": "Chicago"}},
            "thoughtSignature": "signed-function-part"
        }])));
        terminal["candidates"][0]["finishReason"] = json!(native_finish);
        let response = GeminiStreamCase::completed(
            "models/gemini-wire",
            GeminiStreamCase {
                frames: vec![terminal],
            }
            .observe()
            .await,
        );
        assert_eq!(response.finish_reason, Some(expected_finish));
        let [call] = response.message.function_calls() else {
            panic!("the native function call must remain present")
        };
        assert_eq!(call.id(), Some("call_one"));
        assert_eq!(call.name(), "lookup_weather");
        assert_eq!(call.arguments(), &json!({"city": "Chicago"}));
        assert!(response.message.continuation().is_some());
    }
}

#[tokio::test]
async fn eof_deferred_failure_keeps_original_terminal_event_body_and_id() {
    let preceding = GeminiStreamCase::text("earlier-synthetic-accumulated-text");
    let mut absent_content = GeminiStreamCase::terminal(None);
    absent_content["candidates"][0]["content"] = Value::Null;
    let mut invalid_grounding = GeminiStreamCase::terminal(None);
    invalid_grounding["candidates"][0]["groundingMetadata"] = json!({
        "groundingSupports": [{
            "segment": {"startIndex": 0, "endIndex": 7, "text": "earlier"},
            "groundingChunkIndices": [0]
        }]
    });
    for (terminal, reason, details) in [
        (
            absent_content,
            ResponseViolation::InvalidField {
                field: "candidates.content",
            },
            ResponseDetails::FieldType {
                expected: JsonValueKind::Object,
                actual: JsonValueKind::Null,
            },
        ),
        (
            invalid_grounding,
            ResponseViolation::InvalidField {
                field: "groundingChunkIndices",
            },
            ResponseDetails::Index {
                index: 0,
                available: 0,
            },
        ),
    ] {
        let original = terminal.to_string();
        let events = GeminiStreamCase::observe_reply(ScriptedResponse::sse(&format!(
            "data: {preceding}\n\nid: terminal-native-id\ndata: {original}\n\n"
        )))
        .await;
        let error = GeminiStreamCase::observed_error(events, reason);
        assert_eq!(error.response_details(), Some(&details));
        assert_eq!(
            error.native_details().event_id.as_deref(),
            Some("terminal-native-id")
        );
        assert_eq!(
            error.native_details().body.as_deref(),
            Some(original.as_str())
        );
        assert!(!error.native_details().truncated);
        assert!(
            !error
                .native_details()
                .body
                .as_deref()
                .unwrap()
                .contains("earlier-synthetic-accumulated-text")
        );
    }
}

#[tokio::test]
async fn eof_without_finished_candidate_has_no_invented_native_frame() {
    let partial = GeminiStreamCase::text("unterminated answer");
    let events = GeminiStreamCase::observe_reply(ScriptedResponse::sse(&format!(
        "id: unfinished-native-id\ndata: {partial}\n\n"
    )))
    .await;
    let error = GeminiStreamCase::observed_error(events, ResponseViolation::UnexpectedEnd);
    assert_eq!(error.context().phase, ErrorPhase::Stream);
    assert!(error.native_details().body.is_none());
    assert!(error.native_details().event_id.is_none());
}
