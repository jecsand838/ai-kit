use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use providers_kit::error::{ErrorKind, ErrorPhase, Operation, ResponseDetails, ResponseViolation};
use providers_kit::generation::{
    Citation, ContentPart, FileReference, FinishReason, GenerateEvent, GenerateResponse,
};
use providers_kit::hosted::{HostedToolOutput, HostedToolStatus};
use providers_kit::{KitError, ModelBackend, ProviderBackend};
use serde_json::{Value, json};

use crate::support::scripted_http::{ScriptedResponse, spawn_scripted_http_server};
use crate::{WireProvider, WireRequest, WireResponse};

#[tokio::test]
async fn anthropic_fragmented_server_tool_input_preserves_failed_target_and_replay() {
    for initial_input in [None, Some(json!({}))] {
        let url = "https://owned.invalid/fragmented-target";
        let mut invocation = json!({
            "type":"server_tool_use", "id":"srv_fragmented", "name":"web_fetch"
        });
        if let Some(input) = initial_input {
            invocation["input"] = input;
        }
        let result = json!({
            "type":"web_fetch_tool_result", "tool_use_id":"srv_fragmented",
            "content":{"type":"web_fetch_tool_result_error","error_code":"url_not_accessible"}
        });
        let mut fixture = AnthropicStreamFixture::new();
        fixture.block(0, invocation.clone());
        for partial in [
            "{\"url\":\"https://",
            "owned.invalid/fragmented-",
            "target\"}",
        ] {
            fixture.delta(0, json!({"type":"input_json_delta","partial_json":partial}));
        }
        fixture.event(json!({"type":"content_block_stop","index":0}));
        fixture.block(1, result.clone());
        fixture.event(json!({"type":"content_block_stop","index":1}));
        let mut server = spawn_scripted_http_server(vec![
            fixture.finish(),
            WireResponse::anthropic_text("msg_after_fetch", "claude-wire", "continued"),
        ])
        .await;
        let provider = WireProvider::Anthropic.build(server.base_url());
        let model = WireProvider::Anthropic.model(&provider, "claude-wire");
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
                let mut stream = model
                    .generate_stream(&WireRequest::user("Retrieve the exact supplied target."))
                    .await
                    .unwrap();
                let events = stream.by_ref().collect::<Vec<_>>().await;
                let eof = stream.next().await.is_none();
                let (outcome, failures) = AnthropicStreamFixture::stream_outcome(events, eof);
                assert!(failures.is_empty(), "stream contract: {failures:?}");
                let response = outcome.expect("terminal response").unwrap();
                assert_eq!(response.model.as_str(), "claude-wire");
                assert_eq!(response.finish_reason, Some(FinishReason::Stop));
                let [
                    HostedToolOutput::UrlContext {
                        requested_url,
                        retrieved_url,
                        status: Some(HostedToolStatus::Failed { error }),
                        citations,
                    },
                ] = response.hosted_tool_outputs.as_slice()
                else {
                    panic!("expected one correlated failed retrieval");
                };
                assert_eq!(requested_url.as_deref(), Some(url));
                assert!(retrieved_url.is_none());
                assert!(citations.is_empty());
                assert!(response.citations.is_empty());
                assert_eq!(error.kind(), ErrorKind::ProviderRejected);
                assert_eq!(
                    error.native_details().code.as_deref(),
                    Some("url_not_accessible")
                );
                assert_eq!(
                    error.native_details().tool_call_id.as_deref(),
                    Some("srv_fragmented")
                );
                model
                    .generate(&WireRequest::continuation(response.message, "Continue."))
                    .await
                    .unwrap();
                let requests = server.requests();
                assert_eq!(requests.len(), 2);
                let replay: Value = serde_json::from_slice(&requests[1].body).unwrap();
                invocation["input"] = json!({"url":url});
                assert_eq!(
                    replay["messages"][0]["content"],
                    json!([invocation, result])
                );
            }))
            .catch_unwind()
            .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        let joined = server.finish(Duration::from_secs(2)).await;
        if !matches!(&shutdown, Ok(Ok(()))) || joined.is_err() {
            eprintln!("fragmented server input cleanup: shutdown={shutdown:?}; joined={joined:?}");
        }
        match observed {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(result) => result.expect("fragmented server input observation remains bounded"),
        }
        assert!(matches!(shutdown, Ok(Ok(()))), "shutdown={shutdown:?}");
        joined.expect("fragmented server input peer joins");
    }
}

struct AnthropicStreamFixture {
    frames: Vec<Vec<u8>>,
}

impl AnthropicStreamFixture {
    fn new() -> Self {
        let mut fixture = Self { frames: Vec::new() };
        fixture.event(json!({"type":"message_start","message":{
            "id":"msg_citation_boundary","type":"message","role":"assistant",
            "model":"claude-wire","content":[],"stop_reason":null,
            "usage":{"input_tokens":3,"output_tokens":0}
        }}));
        fixture
    }

    fn event(&mut self, value: Value) {
        self.frames.push(
            format!(
                "event: {}\ndata: {value}\n\n",
                value["type"].as_str().unwrap()
            )
            .into_bytes(),
        );
    }

    fn block(&mut self, index: usize, block: Value) {
        self.event(json!({"type":"content_block_start","index":index,"content_block":block}));
    }

    fn delta(&mut self, index: usize, delta: Value) {
        self.event(json!({"type":"content_block_delta","index":index,"delta":delta}));
    }

    fn finish(mut self) -> ScriptedResponse {
        self.event(json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}));
        self.event(json!({"type":"message_stop"}));
        ScriptedResponse::chunks(200, "text/event-stream", self.frames)
    }

    fn citation(index: usize) -> Value {
        json!({
            "type":"web_search_result_location",
            "url":format!("https://www.iana.org/help/example-domains?citation={index}"),
            "title":format!("Example Domains {index}"),
            "cited_text":format!("evidence {index}"),
            "encrypted_index":format!("fixture-encrypted-index-{index}"),
            "native_extra":{"keep":index}
        })
    }

    fn document_citation(kind: &str) -> Value {
        let mut citation = json!({
            "type":kind,
            "cited_text":"native document excerpt",
            "document_index":2,
            "document_title":"Native document title",
            "file_id":"file_native_citation",
            "native_extra":{"keep":[null,7,{"nested":"fixture"}]}
        });
        match kind {
            "char_location" => {
                citation["start_char_index"] = json!(1200);
                citation["end_char_index"] = json!(1250);
            }
            "page_location" => {
                citation["start_page_number"] = json!(3);
                citation["end_page_number"] = json!(5);
            }
            "content_block_location" => {
                citation["start_block_index"] = json!(2);
                citation["end_block_index"] = json!(4);
            }
            _ => panic!("unknown document fixture"),
        }
        citation
    }

    fn search_result_citation() -> Value {
        json!({
            "type":"search_result_location",
            "cited_text":"native search-result excerpt",
            "search_result_index":1,
            "source":"fixture-document-label",
            "title":"Native search-result title",
            "start_block_index":2,
            "end_block_index":4,
            "native_extra":{"keep":true}
        })
    }

    fn citation_reply(citation: &Value, streaming: bool) -> ScriptedResponse {
        if streaming {
            let mut fixture = Self::new();
            fixture.block(0, json!({"type":"text","text":"IANA"}));
            fixture.delta(0, json!({"type":"citations_delta","citation":citation}));
            fixture.event(json!({"type":"content_block_stop","index":0}));
            fixture.finish()
        } else {
            ScriptedResponse::json(
                &json!({
                    "id":"msg_citation_matrix","type":"message","role":"assistant",
                    "model":"claude-wire","stop_reason":"end_turn",
                    "content":[{"type":"text","text":"IANA","citations":[citation]}]
                })
                .to_string(),
            )
        }
    }

    fn stream_outcome(
        events: Vec<Result<GenerateEvent, KitError>>,
        eof: bool,
    ) -> (Option<Result<GenerateResponse, KitError>>, Vec<String>) {
        let mut failures = Vec::new();
        let started = events
            .iter()
            .filter(|event| matches!(event, Ok(GenerateEvent::Started { .. })))
            .count();
        let completed = events
            .iter()
            .filter(|event| matches!(event, Ok(GenerateEvent::Completed(_))))
            .count();
        let errors = events.iter().filter(|event| event.is_err()).count();
        if started != 1 || completed + errors != 1 {
            failures.push(format!(
                "terminal counts: started={started}, completed={completed}, errors={errors}"
            ));
        }
        if !matches!(
            events.last(),
            Some(Ok(GenerateEvent::Completed(_)) | Err(_))
        ) {
            failures.push("terminal result was not the last event".to_owned());
        }
        if !eof {
            failures.push("stream did not remain at EOF".to_owned());
        }
        let outcome = events.into_iter().find_map(|event| match event {
            Ok(GenerateEvent::Completed(response)) => Some(Ok(*response)),
            Err(error) => Some(Err(error)),
            Ok(_) => None,
        });
        (outcome, failures)
    }

    async fn observe_reply(
        reply: ScriptedResponse,
        streaming: bool,
    ) -> (Option<Result<GenerateResponse, KitError>>, Vec<String>) {
        let server = spawn_scripted_http_server(vec![reply]).await;
        let provider = WireProvider::Anthropic.build(server.base_url());
        let model = WireProvider::Anthropic.model(&provider, "claude-wire");
        let request = WireRequest::user("Return cited text.");
        let observed = tokio::time::timeout(Duration::from_secs(10), async {
            if !streaming {
                return (Some(model.generate(&request).await), Vec::new());
            }
            match model.generate_stream(&request).await {
                Ok(mut stream) => {
                    let events = stream.by_ref().collect::<Vec<_>>().await;
                    let eof = stream.next().await.is_none();
                    Self::stream_outcome(events, eof)
                }
                Err(error) => (
                    Some(Err(error)),
                    vec!["fixture stream did not open".to_owned()],
                ),
            }
        })
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        let (outcome, mut failures) = match observed {
            Ok(observed) => observed,
            Err(error) => (None, vec![format!("operation deadline: {error}")]),
        };
        if !matches!(shutdown, Ok(Ok(()))) {
            failures.push(format!("shutdown: {shutdown:?}"));
        }
        if server.requests().len() != 1 {
            failures.push(format!("request count: {}", server.requests().len()));
        }
        (outcome, failures)
    }

    async fn observe_citation(
        citation: &Value,
        streaming: bool,
    ) -> (Option<Result<GenerateResponse, KitError>>, Vec<String>) {
        Self::observe_reply(Self::citation_reply(citation, streaming), streaming).await
    }

    fn portable_citation(citation: &Value) -> Citation {
        if citation["type"] == "web_search_result_location" {
            Citation::Url {
                url: citation["url"].as_str().unwrap().to_owned(),
                title: citation["title"].as_str().map(str::to_owned),
                snippet: citation["cited_text"].as_str().map(str::to_owned),
                start_index: None,
                end_index: None,
            }
        } else {
            Citation::File {
                reference: FileReference::Provider {
                    id: citation["file_id"].as_str().unwrap().to_owned(),
                    media_type: None,
                },
                title: citation["document_title"].as_str().map(str::to_owned),
                snippet: citation["cited_text"].as_str().map(str::to_owned),
                start_index: None,
                end_index: None,
            }
        }
    }

    fn record_error(
        case: &str,
        streaming: bool,
        observation: (Option<Result<GenerateResponse, KitError>>, Vec<String>),
        expected: ErrorKind,
        phase: ErrorPhase,
        failures: &mut Vec<String>,
    ) {
        let (outcome, mut checks) = observation;
        let operation = if streaming {
            Operation::GenerateStream
        } else {
            Operation::Generate
        };
        let matches = matches!(&outcome, Some(Err(error))
            if error.kind() == expected
                && error.context().operation == operation
                && error.context().phase == phase);
        if !matches {
            checks.push("error kind, field, operation, or phase mismatch".to_owned());
        }
        if expected
            == ErrorKind::InvalidResponse(ResponseViolation::UnknownVariant {
                field: "citation.type",
            })
            && !matches!(&outcome, Some(Err(error))
                if matches!(error.response_details(), Some(ResponseDetails::Discriminator { actual })
                    if actual == "future_citation_location"))
        {
            checks.push("unknown discriminator was not retained".to_owned());
        }
        Self::record(
            case,
            streaming,
            format!("{expected:?}/{operation:?}/{phase:?}"),
            format!("{outcome:?}"),
            checks,
            failures,
        );
    }

    fn record_citation(
        case: &str,
        streaming: bool,
        observation: (Option<Result<GenerateResponse, KitError>>, Vec<String>),
        expected: Citation,
        failures: &mut Vec<String>,
    ) {
        let (outcome, mut checks) = observation;
        if !matches!(&outcome, Some(Ok(response))
            if response.citations == [expected.clone()]
                && response.model.as_str() == "claude-wire"
                && response.finish_reason == Some(FinishReason::Stop)
                && response.message.content() == [ContentPart::Text { text: "IANA".to_owned() }])
        {
            checks.push("normalized citation or completed answer mismatch".to_owned());
        }
        Self::record(
            case,
            streaming,
            format!("{expected:?}"),
            format!("{outcome:?}"),
            checks,
            failures,
        );
    }

    fn record(
        case: &str,
        streaming: bool,
        expected: String,
        actual: String,
        checks: Vec<String>,
        failures: &mut Vec<String>,
    ) {
        eprintln!(
            "{}",
            json!({"suite":"anthropic_citation_regression","case":case,
                "mode":if streaming { "stream" } else { "unary" },
                "expected":expected,"actual":actual,"passed":checks.is_empty(),"checks":checks})
        );
        failures.extend(checks.into_iter().map(|check| {
            format!(
                "{case}/{}: {check}",
                if streaming { "stream" } else { "unary" }
            )
        }));
    }

    async fn observe(self) -> Vec<Result<GenerateEvent, KitError>> {
        let server = spawn_scripted_http_server(vec![self.finish()]).await;
        let provider = WireProvider::Anthropic.build(server.base_url());
        let model = WireProvider::Anthropic.model(&provider, "claude-wire");
        let request = WireRequest::user("Return cited text.");
        let observed = tokio::time::timeout(Duration::from_secs(10), async {
            let mut stream = model.generate_stream(&request).await?;
            let events = stream.by_ref().collect::<Vec<_>>().await;
            let eof = stream.next().await.is_none();
            Ok::<_, KitError>((events, eof))
        })
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        assert!(
            matches!(shutdown, Ok(Ok(()))),
            "shutdown={shutdown:?}; observed={observed:?}"
        );
        assert_eq!(server.requests().len(), 1);
        let (events, eof) = observed.expect("bounded stream").expect("stream opens");
        assert!(eof, "stream must stay at EOF");
        events
    }

    async fn assert_error(self, expected: ResponseViolation) {
        let events = self.observe().await;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Ok(GenerateEvent::Completed(_))))
        );
        let errors = events
            .iter()
            .filter_map(|event| event.as_ref().err())
            .collect::<Vec<_>>();
        assert_eq!(errors.len(), 1, "one terminal error: {events:?}");
        assert_eq!(errors[0].kind(), ErrorKind::InvalidResponse(expected));
        assert_eq!(errors[0].context().operation, Operation::GenerateStream);
        assert_eq!(errors[0].context().phase, ErrorPhase::Stream);
        assert!(matches!(events.last(), Some(Err(_))));
    }
}

#[tokio::test]
async fn anthropic_nullable_citations_normalize_in_unary_responses() {
    for citations in [None, Some(Value::Null), Some(json!([]))] {
        let mut block = json!({"type":"text","text":"IANA"});
        if let Some(citations) = citations {
            block["citations"] = citations;
        }
        let native = json!({"id":"msg_nullable_citations","type":"message","role":"assistant","model":"claude-wire","content":[block],"stop_reason":"end_turn"});
        let server =
            spawn_scripted_http_server(vec![ScriptedResponse::json(&native.to_string())]).await;
        let provider = WireProvider::Anthropic.build(server.base_url());
        let model = WireProvider::Anthropic.model(&provider, "claude-wire");
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            model.generate(&WireRequest::user("Cite IANA.")),
        )
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        assert!(
            matches!(shutdown, Ok(Ok(()))),
            "shutdown={shutdown:?}; result={result:?}"
        );
        assert_eq!(server.requests().len(), 1);
        let response = result
            .expect("bounded generation")
            .expect("nullable citations are valid");
        assert_eq!(
            response.message.content(),
            [ContentPart::Text {
                text: "IANA".to_owned()
            }]
        );
        assert_eq!(response.finish_reason, Some(FinishReason::Stop));
        assert!(response.citations.is_empty());
    }
}

#[tokio::test]
async fn anthropic_citation_deltas_preserve_index_order_and_exact_replay() {
    let initial = AnthropicStreamFixture::citation(0);
    let first = AnthropicStreamFixture::citation(1);
    let second = AnthropicStreamFixture::citation(2);
    for initial_citations in [
        None,
        Some(Value::Null),
        Some(json!([])),
        Some(json!([initial])),
    ] {
        let mut block = json!({"type":"text","text":"","native_block":{"keep":true}});
        if let Some(citations) = initial_citations {
            block["citations"] = citations;
        }
        let mut expected_block = block.clone();
        expected_block["text"] = json!("IANA");
        let mut expected_citations = block["citations"].as_array().cloned().unwrap_or_default();
        expected_citations.extend([first.clone(), second.clone()]);
        expected_block["citations"] = json!(expected_citations);
        let prefix = json!({"type":"text","text":"Source: "});
        let mut fixture = AnthropicStreamFixture::new();
        fixture.block(0, prefix.clone());
        fixture.event(json!({"type":"content_block_stop","index":0}));
        fixture.block(1, block);
        fixture.delta(1, json!({"type":"text_delta","text":"IANA"}));
        fixture.delta(1, json!({"type":"citations_delta","citation":first}));
        fixture.delta(1, json!({"type":"citations_delta","citation":second}));
        fixture.event(json!({"type":"content_block_stop","index":1}));
        let server = spawn_scripted_http_server(vec![
            fixture.finish(),
            WireResponse::anthropic_text("msg_replayed", "claude-wire", "continued"),
        ])
        .await;
        let provider = WireProvider::Anthropic.build(server.base_url());
        let model = WireProvider::Anthropic.model(&provider, "claude-wire");
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let request = WireRequest::user("Cite IANA.");
            let mut stream = model.generate_stream(&request).await?;
            let events = stream.by_ref().collect::<Vec<_>>().await;
            let eof = stream.next().await.is_none();
            drop(stream);
            let events = events.into_iter().collect::<Result<Vec<_>, _>>()?;
            Ok::<_, KitError>((events, eof))
        })
        .await;
        let completed = match &result {
            Ok(Ok((events, _))) => events.iter().find_map(|event| match event {
                GenerateEvent::Completed(response) => Some(response),
                _ => None,
            }),
            _ => None,
        };
        let continued = if let Some(response) = completed {
            Some(
                tokio::time::timeout(
                    Duration::from_secs(10),
                    model.generate(&WireRequest::continuation(
                        response.message.clone(),
                        "Continue.",
                    )),
                )
                .await,
            )
        } else {
            None
        };
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        eprintln!(
            "REPLAY_CLEANUP stream={result:?}; continuation={continued:?}; shutdown={shutdown:?}"
        );
        assert!(
            matches!(shutdown, Ok(Ok(()))),
            "shutdown={shutdown:?}; stream={result:?}; continuation={continued:?}"
        );
        let (events, eof) = result
            .expect("bounded stream")
            .expect("citation deltas must normalize");
        let continued = continued
            .expect("citation stream completes before replay")
            .expect("bounded replay")
            .expect("citation continuation must normalize");
        assert!(eof);
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
        let Some(GenerateEvent::Completed(response)) = events.last() else {
            panic!("completion must be final")
        };
        assert_eq!(response.model.as_str(), "claude-wire");
        assert_eq!(response.finish_reason, Some(FinishReason::Stop));
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
            response.message.content(),
            [
                ContentPart::Text {
                    text: "Source: ".to_owned()
                },
                ContentPart::Text {
                    text: "IANA".to_owned()
                },
            ]
        );
        assert_eq!(
            response.citations,
            expected_citations
                .iter()
                .map(|citation| Citation::Url {
                    url: citation["url"].as_str().unwrap().to_owned(),
                    title: Some(citation["title"].as_str().unwrap().to_owned()),
                    snippet: Some(citation["cited_text"].as_str().unwrap().to_owned()),
                    start_index: None,
                    end_index: None,
                })
                .collect::<Vec<_>>()
        );
        assert_eq!(continued.finish_reason, Some(FinishReason::Stop));
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        let replay: Value = serde_json::from_slice(&requests[1].body).unwrap();
        assert_eq!(replay["model"], "claude-wire");
        assert_eq!(
            replay["messages"][0]["content"],
            json!([prefix, expected_block])
        );
    }
}

#[tokio::test]
async fn anthropic_citation_deltas_reject_missing_or_malformed_payloads() {
    let mut failures = Vec::new();
    let mut malformed_url = AnthropicStreamFixture::citation(1);
    malformed_url["url"] = json!(false);
    for (case, delta, expected) in [
        (
            "container/missing",
            json!({"type":"citations_delta"}),
            ResponseViolation::MissingField {
                field: "delta.citation",
            },
        ),
        (
            "container/null",
            json!({"type":"citations_delta","citation":null}),
            ResponseViolation::InvalidField {
                field: "delta.citation",
            },
        ),
        (
            "container/nonobject",
            json!({"type":"citations_delta","citation":"wrong"}),
            ResponseViolation::InvalidField {
                field: "delta.citation",
            },
        ),
        (
            "container/valid_with_invalid_url",
            json!({"type":"citations_delta","citation":malformed_url}),
            ResponseViolation::InvalidField {
                field: "citation.url",
            },
        ),
    ] {
        let mut fixture = AnthropicStreamFixture::new();
        fixture.block(0, json!({"type":"text","text":"IANA"}));
        fixture.delta(0, delta);
        let observation = AnthropicStreamFixture::observe_reply(fixture.finish(), true).await;
        AnthropicStreamFixture::record_error(
            case,
            true,
            observation,
            ErrorKind::InvalidResponse(expected),
            ErrorPhase::Stream,
            &mut failures,
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn anthropic_citation_deltas_reject_invalid_indices_and_nontext_targets() {
    for index in [None, Some(json!(-1)), Some(json!("0")), Some(json!(1))] {
        let mut fixture = AnthropicStreamFixture::new();
        fixture.block(0, json!({"type":"text","text":"IANA"}));
        let expected = match &index {
            None => ResponseViolation::MissingField { field: "index" },
            Some(value) if value == &json!(1) => ResponseViolation::ProtocolViolation {
                rule: "content block must start before deltas",
            },
            Some(_) => ResponseViolation::InvalidField { field: "index" },
        };
        let mut event = json!({"type":"content_block_delta","delta":{"type":"citations_delta","citation":AnthropicStreamFixture::citation(1)}});
        if let Some(index) = index {
            event["index"] = index;
        }
        fixture.event(event);
        fixture.assert_error(expected).await;
    }
    let mut fixture = AnthropicStreamFixture::new();
    fixture.block(
        0,
        json!({"type":"thinking","thinking":"private thought","signature":"fixture-signature"}),
    );
    fixture.delta(
        0,
        json!({"type":"citations_delta","citation":AnthropicStreamFixture::citation(1)}),
    );
    fixture
        .assert_error(ResponseViolation::ProtocolViolation {
            rule: "citation deltas require a text block",
        })
        .await;
}

#[tokio::test]
async fn anthropic_citation_deltas_enforce_retained_item_limit() {
    let mut fixture = AnthropicStreamFixture::new();
    fixture.block(0, json!({"type":"text","text":"IANA"}));
    for index in 0..4096 {
        fixture.delta(
            0,
            json!({"type":"citations_delta","citation":AnthropicStreamFixture::citation(index)}),
        );
    }
    fixture
        .assert_error(ResponseViolation::ProtocolViolation {
            rule: "normalized stream state exceeded its item budget",
        })
        .await;
}

#[tokio::test]
async fn anthropic_citation_deltas_enforce_retained_byte_limit() {
    let mut fixture = AnthropicStreamFixture::new();
    fixture.block(0, json!({"type":"text","text":"IANA"}));
    let mut citation = AnthropicStreamFixture::citation(1);
    citation["cited_text"] = json!("x".repeat(2 * 1024 * 1024));
    for _ in 0..17 {
        fixture.delta(0, json!({"type":"citations_delta","citation":citation}));
    }
    fixture
        .assert_error(ResponseViolation::LimitExceeded {
            limit: 32 * 1024 * 1024,
        })
        .await;
}

#[tokio::test]
async fn anthropic_citation_discriminators_reject_missing_invalid_and_unknown_types() {
    let mut failures = Vec::new();
    for native in [
        AnthropicStreamFixture::citation(1),
        AnthropicStreamFixture::document_citation("char_location"),
        AnthropicStreamFixture::document_citation("page_location"),
        AnthropicStreamFixture::document_citation("content_block_location"),
        AnthropicStreamFixture::search_result_citation(),
    ] {
        let kind = native["type"].as_str().unwrap();
        for (mutation, replacement, violation) in [
            (
                "missing",
                None,
                ResponseViolation::MissingField {
                    field: "citation.type",
                },
            ),
            (
                "null",
                Some(Value::Null),
                ResponseViolation::InvalidField {
                    field: "citation.type",
                },
            ),
            (
                "nonstring",
                Some(json!(17)),
                ResponseViolation::InvalidField {
                    field: "citation.type",
                },
            ),
            (
                "unknown",
                Some(json!("future_citation_location")),
                ResponseViolation::UnknownVariant {
                    field: "citation.type",
                },
            ),
        ] {
            let mut citation = native.clone();
            match replacement {
                Some(value) => citation["type"] = value,
                None => {
                    citation.as_object_mut().unwrap().remove("type");
                }
            }
            for streaming in [false, true] {
                let observation =
                    AnthropicStreamFixture::observe_citation(&citation, streaming).await;
                AnthropicStreamFixture::record_error(
                    &format!("discriminator/{kind}/{mutation}"),
                    streaming,
                    observation,
                    ErrorKind::InvalidResponse(violation),
                    if streaming {
                        ErrorPhase::Stream
                    } else {
                        ErrorPhase::Normalize
                    },
                    &mut failures,
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn anthropic_citations_require_documented_string_and_index_fields() {
    let mut failures = Vec::new();
    for (native, fields) in [
        (
            AnthropicStreamFixture::citation(1),
            vec![
                ("url", "citation.url", false),
                ("cited_text", "citation.cited_text", false),
                ("encrypted_index", "citation.encrypted_index", false),
            ],
        ),
        (
            AnthropicStreamFixture::document_citation("char_location"),
            vec![
                ("cited_text", "citation.cited_text", false),
                ("document_index", "citation.document_index", true),
                ("start_char_index", "citation.start_char_index", true),
                ("end_char_index", "citation.end_char_index", true),
            ],
        ),
        (
            AnthropicStreamFixture::document_citation("page_location"),
            vec![
                ("cited_text", "citation.cited_text", false),
                ("document_index", "citation.document_index", true),
                ("start_page_number", "citation.start_page_number", true),
                ("end_page_number", "citation.end_page_number", true),
            ],
        ),
        (
            AnthropicStreamFixture::document_citation("content_block_location"),
            vec![
                ("cited_text", "citation.cited_text", false),
                ("document_index", "citation.document_index", true),
                ("start_block_index", "citation.start_block_index", true),
                ("end_block_index", "citation.end_block_index", true),
            ],
        ),
        (
            AnthropicStreamFixture::search_result_citation(),
            vec![
                ("cited_text", "citation.cited_text", false),
                ("source", "citation.source", false),
                ("search_result_index", "citation.search_result_index", true),
                ("start_block_index", "citation.start_block_index", true),
                ("end_block_index", "citation.end_block_index", true),
            ],
        ),
    ] {
        let kind = native["type"].as_str().unwrap();
        for (field, label, numeric) in fields {
            for (mutation, replacement) in [
                ("missing", None),
                ("null", Some(Value::Null)),
                ("boolean", Some(json!(false))),
                (
                    "wrong_scalar",
                    Some(if numeric { json!("wrong") } else { json!(17) }),
                ),
                ("array", Some(json!([]))),
                ("object", Some(json!({}))),
            ] {
                let violation = if replacement.is_none() {
                    ResponseViolation::MissingField { field: label }
                } else {
                    ResponseViolation::InvalidField { field: label }
                };
                let mut citation = native.clone();
                match replacement {
                    Some(value) => citation[field] = value,
                    None => {
                        citation.as_object_mut().unwrap().remove(field);
                    }
                }
                for streaming in [false, true] {
                    let observation =
                        AnthropicStreamFixture::observe_citation(&citation, streaming).await;
                    AnthropicStreamFixture::record_error(
                        &format!("required/{kind}/{field}/{mutation}"),
                        streaming,
                        observation,
                        ErrorKind::InvalidResponse(violation),
                        if streaming {
                            ErrorPhase::Stream
                        } else {
                            ErrorPhase::Normalize
                        },
                        &mut failures,
                    );
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn anthropic_citation_optional_fields_accept_null_and_reject_wrong_types() {
    let mut failures = Vec::new();
    for (native, title_field, title_label) in [
        (
            AnthropicStreamFixture::citation(1),
            "title",
            "citation.title",
        ),
        (
            AnthropicStreamFixture::document_citation("char_location"),
            "document_title",
            "citation.document_title",
        ),
        (
            AnthropicStreamFixture::document_citation("page_location"),
            "document_title",
            "citation.document_title",
        ),
        (
            AnthropicStreamFixture::document_citation("content_block_location"),
            "document_title",
            "citation.document_title",
        ),
        (
            AnthropicStreamFixture::search_result_citation(),
            "title",
            "citation.title",
        ),
    ] {
        let kind = native["type"].as_str().unwrap();
        for (mutation, replacement) in [
            ("missing", None),
            ("null", Some(Value::Null)),
            ("string", Some(json!("Fixture title"))),
        ] {
            let mut citation = native.clone();
            match replacement {
                Some(value) => citation[title_field] = value,
                None => {
                    citation.as_object_mut().unwrap().remove(title_field);
                }
            }
            for streaming in [false, true] {
                let observation =
                    AnthropicStreamFixture::observe_citation(&citation, streaming).await;
                let case = format!("optional/{kind}/{title_field}/{mutation}");
                if kind == "search_result_location" {
                    AnthropicStreamFixture::record_error(
                        &case,
                        streaming,
                        observation,
                        ErrorKind::Unsupported,
                        ErrorPhase::Normalize,
                        &mut failures,
                    );
                } else {
                    AnthropicStreamFixture::record_citation(
                        &case,
                        streaming,
                        observation,
                        AnthropicStreamFixture::portable_citation(&citation),
                        &mut failures,
                    );
                }
            }
        }
        let mut fields = vec![(title_field, title_label)];
        if native.get("file_id").is_some() {
            fields.push(("file_id", "citation.file_id"));
        }
        for (field, label) in fields {
            for (mutation, value) in [
                ("boolean", json!(false)),
                ("number", json!(17)),
                ("array", json!([])),
                ("object", json!({})),
            ] {
                let mut citation = native.clone();
                citation[field] = value;
                for streaming in [false, true] {
                    let observation =
                        AnthropicStreamFixture::observe_citation(&citation, streaming).await;
                    AnthropicStreamFixture::record_error(
                        &format!("optional/{kind}/{field}/{mutation}"),
                        streaming,
                        observation,
                        ErrorKind::InvalidResponse(ResponseViolation::InvalidField {
                            field: label,
                        }),
                        if streaming {
                            ErrorPhase::Stream
                        } else {
                            ErrorPhase::Normalize
                        },
                        &mut failures,
                    );
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn anthropic_document_citations_preserve_native_file_ids_without_output_offsets() {
    let mut failures = Vec::new();
    for kind in ["char_location", "page_location", "content_block_location"] {
        let citation = AnthropicStreamFixture::document_citation(kind);
        for streaming in [false, true] {
            let observation = AnthropicStreamFixture::observe_citation(&citation, streaming).await;
            AnthropicStreamFixture::record_citation(
                &format!("native_file/{kind}"),
                streaming,
                observation,
                AnthropicStreamFixture::portable_citation(&citation),
                &mut failures,
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn anthropic_index_only_citations_are_unsupported_at_normalization() {
    let mut failures = Vec::new();
    for kind in ["char_location", "page_location", "content_block_location"] {
        for null in [false, true] {
            let mut citation = AnthropicStreamFixture::document_citation(kind);
            citation.as_object_mut().unwrap().remove("file_id");
            if null {
                citation["file_id"] = Value::Null;
            }
            for streaming in [false, true] {
                let observation =
                    AnthropicStreamFixture::observe_citation(&citation, streaming).await;
                AnthropicStreamFixture::record_error(
                    &format!(
                        "index_only/{kind}/{}",
                        if null { "null" } else { "missing" }
                    ),
                    streaming,
                    observation,
                    ErrorKind::Unsupported,
                    ErrorPhase::Normalize,
                    &mut failures,
                );
            }
        }
    }
    for source in [
        "fixture-document-label",
        "https://example.com/native-search-source",
    ] {
        let mut citation = AnthropicStreamFixture::search_result_citation();
        citation["source"] = json!(source);
        for streaming in [false, true] {
            let observation = AnthropicStreamFixture::observe_citation(&citation, streaming).await;
            AnthropicStreamFixture::record_error(
                &format!("index_only/search_result_location/{source}"),
                streaming,
                observation,
                ErrorKind::Unsupported,
                ErrorPhase::Normalize,
                &mut failures,
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn anthropic_citation_discriminators_control_attribution_despite_extra_fields() {
    let mut failures = Vec::new();
    let mut web = AnthropicStreamFixture::citation(1);
    web["file_id"] = json!("file_unrelated_extension");
    for streaming in [false, true] {
        let observation = AnthropicStreamFixture::observe_citation(&web, streaming).await;
        AnthropicStreamFixture::record_citation(
            "extra/web_search/file_id",
            streaming,
            observation,
            AnthropicStreamFixture::portable_citation(&web),
            &mut failures,
        );
    }
    for kind in ["char_location", "page_location", "content_block_location"] {
        let mut citation = AnthropicStreamFixture::document_citation(kind);
        citation.as_object_mut().unwrap().remove("file_id");
        citation["url"] = json!("https://example.com/unrelated-extension");
        for streaming in [false, true] {
            let observation = AnthropicStreamFixture::observe_citation(&citation, streaming).await;
            AnthropicStreamFixture::record_error(
                &format!("extra/{kind}/url"),
                streaming,
                observation,
                ErrorKind::Unsupported,
                ErrorPhase::Normalize,
                &mut failures,
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn anthropic_citation_source_ranges_reject_invalid_indices() {
    let mut failures = Vec::new();
    for (native, source_field, source_label, start_field, start_label, end_field, end_label) in [
        (
            AnthropicStreamFixture::document_citation("char_location"),
            "document_index",
            "citation.document_index",
            "start_char_index",
            "citation.start_char_index",
            "end_char_index",
            "citation.end_char_index",
        ),
        (
            AnthropicStreamFixture::document_citation("page_location"),
            "document_index",
            "citation.document_index",
            "start_page_number",
            "citation.start_page_number",
            "end_page_number",
            "citation.end_page_number",
        ),
        (
            AnthropicStreamFixture::document_citation("content_block_location"),
            "document_index",
            "citation.document_index",
            "start_block_index",
            "citation.start_block_index",
            "end_block_index",
            "citation.end_block_index",
        ),
        (
            AnthropicStreamFixture::search_result_citation(),
            "search_result_index",
            "citation.search_result_index",
            "start_block_index",
            "citation.start_block_index",
            "end_block_index",
            "citation.end_block_index",
        ),
    ] {
        let kind = native["type"].as_str().unwrap();
        let mut mutations = vec![
            ("negative_source", source_field, source_label, json!(-1)),
            ("negative_start", start_field, start_label, json!(-1)),
            ("negative_end", end_field, end_label, json!(-1)),
            (
                "reversed",
                end_field,
                end_label,
                json!(native[start_field].as_i64().unwrap() - 1),
            ),
        ];
        if kind == "page_location" {
            mutations.push(("page_zero", start_field, start_label, json!(0)));
        }
        if matches!(kind, "content_block_location" | "search_result_location") {
            mutations.push((
                "equal_blocks",
                end_field,
                end_label,
                native[start_field].clone(),
            ));
        }
        for (mutation, field, label, value) in mutations {
            let mut citation = native.clone();
            citation[field] = value;
            for streaming in [false, true] {
                let observation =
                    AnthropicStreamFixture::observe_citation(&citation, streaming).await;
                AnthropicStreamFixture::record_error(
                    &format!("range/{kind}/{mutation}"),
                    streaming,
                    observation,
                    ErrorKind::InvalidResponse(ResponseViolation::InvalidField { field: label }),
                    ErrorPhase::Normalize,
                    &mut failures,
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn anthropic_initial_block_citations_validate_discriminator() {
    let mut citation = AnthropicStreamFixture::citation(1);
    citation.as_object_mut().unwrap().remove("type");
    let mut fixture = AnthropicStreamFixture::new();
    fixture.block(
        0,
        json!({"type":"text","text":"IANA","citations":[citation]}),
    );
    fixture.event(json!({"type":"content_block_stop","index":0}));
    let observation = AnthropicStreamFixture::observe_reply(fixture.finish(), true).await;
    let mut failures = Vec::new();
    AnthropicStreamFixture::record_error(
        "initial_block/missing_type",
        true,
        observation,
        ErrorKind::InvalidResponse(ResponseViolation::MissingField {
            field: "citation.type",
        }),
        ErrorPhase::Normalize,
        &mut failures,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn anthropic_file_citations_preserve_exact_raw_replay_in_unary_and_streaming() {
    let mut failures = Vec::new();
    for kind in ["char_location", "page_location", "content_block_location"] {
        let mut citation = AnthropicStreamFixture::document_citation(kind);
        citation["url"] = json!("https://example.com/unrelated-file-extension");
        for streaming in [false, true] {
            let server = spawn_scripted_http_server(vec![
                AnthropicStreamFixture::citation_reply(&citation, streaming),
                WireResponse::anthropic_text("msg_file_replayed", "claude-wire", "continued"),
            ])
            .await;
            let provider = WireProvider::Anthropic.build(server.base_url());
            let model = WireProvider::Anthropic.model(&provider, "claude-wire");
            let request = WireRequest::user("Return cited text.");
            let observed = tokio::time::timeout(Duration::from_secs(10), async {
                if !streaming {
                    return (Some(model.generate(&request).await), Vec::new());
                }
                match model.generate_stream(&request).await {
                    Ok(mut stream) => {
                        let events = stream.by_ref().collect::<Vec<_>>().await;
                        let eof = stream.next().await.is_none();
                        AnthropicStreamFixture::stream_outcome(events, eof)
                    }
                    Err(error) => (
                        Some(Err(error)),
                        vec!["fixture stream did not open".to_owned()],
                    ),
                }
            })
            .await;
            let (outcome, mut checks) = match observed {
                Ok(observation) => observation,
                Err(error) => (None, vec![format!("first-operation deadline: {error}")]),
            };
            let continued = if let Some(Ok(response)) = &outcome {
                Some(
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        model.generate(&WireRequest::continuation(
                            response.message.clone(),
                            "Continue.",
                        )),
                    )
                    .await,
                )
            } else {
                None
            };
            let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
            if !matches!(shutdown, Ok(Ok(()))) {
                checks.push(format!("shutdown: {shutdown:?}"));
            }
            let requests = server.requests();
            if requests.len() != 2 {
                checks.push(format!("replay request count: {}", requests.len()));
            }
            let replay = requests
                .get(1)
                .map(|request| serde_json::from_slice::<Value>(&request.body));
            let expected_content = json!([{"type":"text","text":"IANA","citations":[citation]}]);
            if !matches!(&replay, Some(Ok(body))
                if body["model"] == "claude-wire" && body["messages"][0]["content"] == expected_content)
            {
                checks.push("raw citation replay changed fields or source positions".to_owned());
            }
            if !matches!(&continued, Some(Ok(Ok(response)))
                if response.model.as_str() == "claude-wire" && response.finish_reason == Some(FinishReason::Stop))
            {
                checks.push(format!("continuation result: {continued:?}"));
            }
            AnthropicStreamFixture::record(
                &format!("file_replay/{kind}"),
                streaming,
                expected_content.to_string(),
                format!("{replay:?}"),
                checks,
                &mut failures,
            );
            AnthropicStreamFixture::record_citation(
                &format!("file_replay/{kind}/normalized"),
                streaming,
                (outcome, Vec::new()),
                AnthropicStreamFixture::portable_citation(&citation),
                &mut failures,
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
