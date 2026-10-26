mod anthropic_stream;
mod current_anthropic_gemini;
mod current_openai_ollama;
mod gemini_batch;
mod gemini_stream;
mod openai_audio;
mod openai_batch;

use std::panic::{AssertUnwindSafe, resume_unwind};
use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use provider_sdk::error::{ClassificationBasis, ErrorKind, ResponseDetails, ResponseViolation};
use provider_sdk::generation::{FinishReason, GenerateEvent, GenerateResponse, ProviderToolSpec};
use provider_sdk::hosted::{HostedTool, HostedToolOutput, HostedToolStatus};
use provider_sdk::{ModelBackend, ProviderBackend, SdkError};
use serde_json::{Value, json};

use super::{WireProvider, WireRequest};
use crate::support::scripted_http::{ScriptedResponse, spawn_scripted_http_server};

struct HostedWireCase {
    provider: WireProvider,
    tool: HostedTool,
    native: Value,
}

impl HostedWireCase {
    const URL: &str = "https://www.iana.org/help/example-domains";

    fn new(provider: WireProvider, tool: HostedTool, native: Value) -> Self {
        Self {
            provider,
            tool,
            native,
        }
    }

    fn openai(items: Vec<Value>) -> Value {
        json!({"model":"gpt-4.1","status":"completed","output":items})
    }

    fn anthropic(content: Vec<Value>, stop: &str) -> Value {
        json!({"id":"msg_hosted","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":content,"stop_reason":stop,"usage":{"input_tokens":2,"output_tokens":3}})
    }

    fn gemini(parts: Vec<Value>) -> Value {
        json!({"modelVersion":"models/gemini-2.5-flash","candidates":[{"content":{"role":"model","parts":parts},"finishReason":"STOP"}]})
    }

    fn reply(&self, streaming: bool) -> ScriptedResponse {
        if !streaming {
            return ScriptedResponse::json(&self.native.to_string());
        }
        let frames = match self.provider {
            WireProvider::OpenAi => format!(
                "data: {}\n\n",
                json!({"type":"response.completed","response":self.native})
            ),
            WireProvider::Gemini => format!("data: {}\n\n", self.native),
            WireProvider::Anthropic => {
                let mut start = self.native.clone();
                start["content"] = json!([]);
                start["stop_reason"] = Value::Null;
                let mut frames = format!(
                    "event: message_start\ndata: {}\n\n",
                    json!({"type":"message_start","message":start})
                );
                for (index, block) in self.native["content"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .enumerate()
                {
                    frames.push_str(&format!(
                        "event: content_block_start\ndata: {}\n\n",
                        json!({"type":"content_block_start","index":index,"content_block":block})
                    ));
                    frames.push_str(&format!(
                        "event: content_block_stop\ndata: {}\n\n",
                        json!({"type":"content_block_stop","index":index})
                    ));
                }
                frames.push_str(&format!("event: message_delta\ndata: {}\n\n", json!({"type":"message_delta","delta":{"stop_reason":self.native["stop_reason"]}})));
                frames.push_str("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
                frames
            }
            WireProvider::Ollama => panic!("Ollama has no supported hosted output"),
        };
        ScriptedResponse::sse(&frames)
    }

    async fn generate(self, streaming: bool) -> Result<GenerateResponse, SdkError> {
        let mut server = spawn_scripted_http_server(vec![self.reply(streaming)]).await;
        let provider = self.provider.build(server.base_url());
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            let selected = match self.provider {
                WireProvider::OpenAi => "gpt-4.1",
                WireProvider::Anthropic => "claude-sonnet-4-6",
                WireProvider::Gemini => "models/gemini-2.5-flash",
                WireProvider::Ollama => panic!("Ollama has no supported hosted output"),
            };
            let model = self.provider.model(&provider, selected);
            let request = WireRequest::user("Exercise the configured hosted tool.")
                .with_tools(vec![ProviderToolSpec::Hosted(self.tool)])
                .unwrap();
            if streaming {
                match model.generate_stream(&request).await {
                    Err(error) => (Some(Err(error)), true),
                    Ok(mut events) => {
                        let completed = loop {
                            match events.next().await {
                                Some(Ok(GenerateEvent::Completed(response))) => {
                                    break Some(Ok(*response));
                                }
                                Some(Err(error)) => break Some(Err(error)),
                                Some(Ok(_)) => {}
                                None => break None,
                            }
                        };
                        let eof = events.next().await.is_none();
                        (completed, eof)
                    }
                }
            } else {
                (Some(model.generate(&request).await), true)
            }
        }))
        .catch_unwind()
        .await;
        let shutdown = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(10),
            provider.shutdown(),
        ))
        .catch_unwind()
        .await;
        let settled = server.finish(Duration::from_secs(2)).await;
        let observed = match observed {
            Ok(observed) => observed,
            Err(panic) => {
                eprintln!("hosted observation panicked; shutdown={shutdown:?}; server={settled:?}");
                resume_unwind(panic);
            }
        };
        let shutdown = match shutdown {
            Ok(shutdown) => shutdown,
            Err(panic) => {
                eprintln!("hosted shutdown panicked; observed={observed:?}; server={settled:?}");
                resume_unwind(panic);
            }
        };
        assert!(
            matches!(shutdown, Ok(Ok(()))) && settled.is_ok(),
            "shutdown={shutdown:?}; server={settled:?}; observed={observed:?}"
        );
        assert_eq!(
            server.requests().len(),
            1,
            "shutdown={shutdown:?}; server={settled:?}; observed={observed:?}"
        );
        assert!(
            observed.is_ok(),
            "hosted operation exceeded ten seconds; observed={observed:?}; shutdown={shutdown:?}; server={settled:?}"
        );
        let (result, eof) = observed.unwrap();
        assert!(
            eof,
            "hosted stream must remain at EOF; result={result:?}; shutdown={shutdown:?}; server={settled:?}"
        );
        assert!(
            result.is_some(),
            "hosted stream omitted a terminal result; shutdown={shutdown:?}; server={settled:?}"
        );
        result.unwrap()
    }
}

#[tokio::test]
async fn openai_hosted_actions_preserve_search_open_and_find_semantics() {
    for action in [
        json!({"type":"search","query":"IANA example domains","queries":["IANA example domains"],"sources":[{"type":"url","url":HostedWireCase::URL}]}),
        json!({"type":"open_page","url":HostedWireCase::URL}),
        json!({"type":"find_in_page","url":HostedWireCase::URL,"pattern":"IANA"}),
    ] {
        for streaming in [false, true] {
            let response = HostedWireCase::new(WireProvider::OpenAi, HostedTool::WebSearch,
                HostedWireCase::openai(vec![json!({"type":"web_search_call","id":"ws_action","status":"completed","action":action})]))
                .generate(streaming).await.unwrap();
            let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
            assert_eq!(output["action"]["type"], action["type"]);
            match action["type"].as_str().unwrap() {
                "search" => {
                    assert_eq!(output["action"]["query"], action["query"]);
                    assert_eq!(output["action"]["queries"], action["queries"]);
                    assert_eq!(output["action"]["sources"][0]["url"], HostedWireCase::URL);
                }
                "open_page" => assert_eq!(output["action"]["url"], HostedWireCase::URL),
                "find_in_page" => {
                    assert_eq!(output["action"]["url"], HostedWireCase::URL);
                    assert_eq!(output["action"]["pattern"], "IANA");
                }
                _ => unreachable!(),
            }
        }
    }
}

#[tokio::test]
async fn openai_hosted_action_and_output_unknown_variants_are_not_projected_as_known() {
    for (tool, item, field) in [
        (
            HostedTool::WebSearch,
            json!({"type":"web_search_call","id":"ws_unknown","status":"completed","action":{"type":"future_action","query":"retained private query"}}),
            "action.type",
        ),
        (
            HostedTool::CodeExecution,
            json!({"type":"code_interpreter_call","id":"ci_unknown","status":"completed","container_id":"container_owned","code":"print(1)","outputs":[{"type":"code","code":"print(2)"}]}),
            "outputs.type",
        ),
    ] {
        let error = HostedWireCase::new(
            WireProvider::OpenAi,
            tool,
            HostedWireCase::openai(vec![item]),
        )
        .generate(false)
        .await
        .unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::InvalidResponse(ResponseViolation::UnknownVariant { field })
        );
    }
}

#[tokio::test]
async fn openai_consulted_results_do_not_manufacture_answer_citations() {
    let response = HostedWireCase::new(WireProvider::OpenAi, HostedTool::WebSearch,
        HostedWireCase::openai(vec![json!({"type":"web_search_call","id":"ws_sources","status":"completed","action":{"type":"search","query":"IANA"},"results":[{"url":HostedWireCase::URL,"title":"Example Domains"}]})]))
        .generate(false).await.unwrap();
    let HostedToolOutput::WebSearch {
        results, citations, ..
    } = &response.hosted_tool_outputs[0]
    else {
        panic!("web search output");
    };
    assert_eq!(results.len(), 1);
    assert!(citations.is_empty());
    assert!(response.citations.is_empty());
}

#[tokio::test]
async fn openai_interpreter_preserves_logs_without_claiming_stdout_or_exit_codes() {
    for streaming in [false, true] {
        let response = HostedWireCase::new(WireProvider::OpenAi, HostedTool::CodeExecution,
            HostedWireCase::openai(vec![json!({"type":"code_interpreter_call","id":"ci_logs","container_id":"container_owned","status":"completed","code":"print(1)","outputs":[{"type":"logs","logs":"combined native log\n"}]})]))
            .generate(streaming).await.unwrap();
        let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
        assert_eq!(output["results"][0]["logs"], "combined native log\n");
        assert_eq!(output["results"][0]["stdout"], Value::Null);
        assert_eq!(output["results"][0]["stderr"], Value::Null);
        assert_eq!(output["results"][0]["exit_code"], Value::Null);
        assert_eq!(output["results"][0]["status"], Value::Null);
    }
}

#[tokio::test]
async fn openai_interpreter_null_outputs_and_non_success_lifecycle_stay_distinct() {
    for (native_status, portable_status) in [
        ("interpreting", "in_progress"),
        ("incomplete", "incomplete"),
        ("completed", "completed"),
    ] {
        for streaming in [false, true] {
            let response = HostedWireCase::new(WireProvider::OpenAi, HostedTool::CodeExecution,
                HostedWireCase::openai(vec![json!({"type":"code_interpreter_call","id":"ci_empty","container_id":"container_owned","status":native_status,"code":"print(1)","outputs":null})]))
                .generate(streaming).await.unwrap();
            let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
            assert_eq!(output["status"]["state"], portable_status);
            assert_eq!(output["results"], json!([]));
        }
    }
}

impl HostedWireCase {
    async fn assert_openai_interpreting_stream(complete: bool) {
        let created = json!({
            "type":"response.created", "sequence_number":0,
            "response":{"id":"resp_wire","model":"gpt-4.1","status":"in_progress","output":[]}
        });
        let interpreting = json!({
            "type":"response.code_interpreter_call.interpreting",
            "item_id":"ci_wire", "output_index":0, "sequence_number":1
        });
        let mut frames = format!(
            "event: response.created\ndata: {created}\n\nevent: response.code_interpreter_call.interpreting\ndata: {interpreting}\n\n"
        );
        if complete {
            let completed = json!({
                "type":"response.completed", "sequence_number":2,
                "response":HostedWireCase::openai(vec![json!({
                    "type":"code_interpreter_call", "id":"ci_wire",
                    "container_id":"container_wire", "status":"completed",
                    "code":"print(1)", "outputs":[{"type":"logs","logs":"1\n"}]
                })])
            });
            frames.push_str(&format!("event: response.completed\ndata: {completed}\n\n"));
        }
        let server = spawn_scripted_http_server(vec![ScriptedResponse::sse(&frames)]).await;
        let provider = WireProvider::OpenAi.build(server.base_url());
        let model = WireProvider::OpenAi.model(&provider, "gpt-4.1");
        let request = WireRequest::user("Run print(1).")
            .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::CodeExecution)])
            .unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(10), async {
            let mut stream = model.generate_stream(&request).await?;
            let events = stream.by_ref().collect::<Vec<_>>().await;
            let eof = stream.next().await.is_none();
            Ok::<_, SdkError>((events, eof))
        })
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        let actual_events = match &observed {
            Ok(Ok((events, _))) => json!(
                events
                    .iter()
                    .map(|event| match event {
                        Ok(GenerateEvent::Started { model }) => json!({
                            "event":"started", "model":model.as_str()
                        }),
                        Ok(GenerateEvent::Completed(response)) => json!({
                            "event":"completed", "model":response.model.as_str(),
                            "finish_reason":format!("{:?}", response.finish_reason),
                            "outputs":response.hosted_tool_outputs,
                            "citations":response.citations, "artifacts":response.artifacts
                        }),
                        Ok(event) => json!({"unexpected_event":format!("{event:?}")}),
                        Err(error) => json!({
                            "event":"error", "kind":format!("{:?}", error.kind()),
                            "operation":format!("{:?}", error.context().operation),
                            "phase":format!("{:?}", error.context().phase),
                            "http_status":error.http_status()
                        }),
                    })
                    .collect::<Vec<_>>()
            ),
            other => json!({"operation_failure":format!("{other:?}")}),
        };
        let expected_events = if complete {
            json!([{"event":"started","model":"gpt-4.1"},{
                "event":"completed", "model":"gpt-4.1", "finish_reason":"Some(Stop)",
                "outputs":[{
                    "family":"code_execution", "status":{"state":"completed"},
                    "code":[{"language":null,"code":"print(1)"}],
                    "results":[{
                        "status":null,"logs":"1\n","stdout":null,"stderr":null,"exit_code":null
                    }],
                    "artifacts":[]
                }],
                "citations":[], "artifacts":[]
            }])
        } else {
            json!([{"event":"started","model":"gpt-4.1"},{
                "event":"error",
                "kind":format!("{:?}", ErrorKind::InvalidResponse(ResponseViolation::UnexpectedEnd)),
                "operation":"GenerateStream", "phase":"Stream", "http_status":null
            }])
        };
        let actual = json!({
            "events":actual_events,
            "eof":matches!(&observed, Ok(Ok((_, true)))),
            "shutdown":format!("{shutdown:?}"),
            "requests":server.requests().iter().map(|request| {
                json!({"method":request.method,"path":request.path})
            }).collect::<Vec<_>>()
        });
        let expected = json!({
            "events":expected_events, "eof":true, "shutdown":"Ok(Ok(()))",
            "requests":[{"method":"POST","path":"/responses"}]
        });
        let evidence = json!({
            "test":if complete {
                "openai_interpreting_stream_frame_preserves_completed_code_output"
            } else {
                "openai_interpreting_stream_frame_without_completion_is_unexpected_end"
            },
            "variant":if complete { "completed" } else { "interpreting_only_eof" },
            "expected":expected, "actual":actual,
            "shutdown_passed":matches!(shutdown, Ok(Ok(()))),
            "passed":actual == expected
        });
        println!("REGRESSION_CASE {evidence}");
        assert_eq!(actual, expected, "hosted stream fixture failed: {evidence}");
    }
}

#[tokio::test]
async fn openai_interpreting_stream_frame_preserves_completed_code_output() {
    HostedWireCase::assert_openai_interpreting_stream(true).await;
}

#[tokio::test]
async fn openai_interpreting_stream_frame_without_completion_is_unexpected_end() {
    HostedWireCase::assert_openai_interpreting_stream(false).await;
}

#[tokio::test]
async fn anthropic_search_joins_only_its_matching_invocation_and_keeps_genuine_citations() {
    let native = HostedWireCase::anthropic(
        vec![
            json!({"type":"server_tool_use","id":"srv_search","name":"web_search","input":{"query":"IANA example domains"}}),
            json!({"type":"web_search_tool_result","tool_use_id":"srv_search","content":[{"type":"web_search_result","url":HostedWireCase::URL,"title":"Example Domains","encrypted_content":"native replay bytes"}]}),
            json!({"type":"text","text":"IANA","citations":[{"type":"web_search_result_location","url":HostedWireCase::URL,"title":"Example Domains","encrypted_index":"native citation index","cited_text":"IANA"}]}),
        ],
        "end_turn",
    );
    for streaming in [false, true] {
        let response = HostedWireCase::new(
            WireProvider::Anthropic,
            HostedTool::WebSearch,
            native.clone(),
        )
        .generate(streaming)
        .await
        .unwrap();
        assert_eq!(response.hosted_tool_outputs.len(), 1);
        let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
        assert_eq!(output["status"]["state"], "completed");
        assert_eq!(output["action"]["type"], "search");
        assert_eq!(output["action"]["query"], "IANA example domains");
        assert_eq!(output["results"][0]["url"], HostedWireCase::URL);
        assert_eq!(response.citations.len(), 1);
    }
}

#[tokio::test]
async fn anthropic_known_contradictory_or_duplicate_result_links_are_invalid() {
    let invocation = json!({"type":"server_tool_use","id":"srv_one","name":"web_search","input":{"query":"IANA"}});
    let result = json!({"type":"web_search_tool_result","tool_use_id":"srv_one","content":[]});
    for blocks in [
        vec![invocation.clone(), invocation.clone()],
        vec![invocation.clone(), result.clone(), result],
        vec![
            invocation,
            json!({"type":"bash_code_execution_tool_result","tool_use_id":"srv_one","content":{"type":"bash_code_execution_result","stdout":"1","stderr":"","return_code":0,"content":[]}}),
        ],
    ] {
        let error = HostedWireCase::new(
            WireProvider::Anthropic,
            HostedTool::WebSearch,
            HostedWireCase::anthropic(blocks, "end_turn"),
        )
        .generate(false)
        .await
        .unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::InvalidResponse(_)));
    }
}

#[tokio::test]
async fn anthropic_pending_and_prior_turn_result_only_outputs_remain_honest() {
    let invocation = json!({"type":"server_tool_use","id":"srv_previous","name":"web_search","input":{"query":"IANA"}});
    let result = json!({"type":"web_search_tool_result","tool_use_id":"srv_previous","content":[{"type":"web_search_result","url":HostedWireCase::URL,"title":"Example Domains","encrypted_content":"opaque evidence"}]});
    for streaming in [false, true] {
        let pending = HostedWireCase::new(
            WireProvider::Anthropic,
            HostedTool::WebSearch,
            HostedWireCase::anthropic(vec![invocation.clone()], "pause_turn"),
        )
        .generate(streaming)
        .await
        .unwrap();
        assert_eq!(pending.finish_reason, Some(FinishReason::Paused));
        assert!(
            matches!(&pending.hosted_tool_outputs[0], HostedToolOutput::WebSearch {status:Some(HostedToolStatus::InProgress),results,..} if results.is_empty())
        );
        let completed = HostedWireCase::new(
            WireProvider::Anthropic,
            HostedTool::WebSearch,
            HostedWireCase::anthropic(vec![result.clone()], "end_turn"),
        )
        .generate(streaming)
        .await
        .unwrap();
        assert!(
            matches!(&completed.hosted_tool_outputs[0], HostedToolOutput::WebSearch {status:Some(HostedToolStatus::Completed),action:None,results,..} if results.len()==1)
        );
    }
}

#[tokio::test]
async fn anthropic_correlated_hosted_output_preserves_raw_continuation() {
    let content = vec![
        json!({"type":"server_tool_use","id":"srv_replay","name":"web_search","input":{"query":"IANA"}}),
        json!({"type":"web_search_tool_result","tool_use_id":"srv_replay","content":[{"type":"web_search_result","url":HostedWireCase::URL,"title":"Example Domains","encrypted_content":"exact opaque native evidence"}]}),
    ];
    let server = spawn_scripted_http_server(vec![
        ScriptedResponse::json(&HostedWireCase::anthropic(content.clone(), "end_turn").to_string()),
        ScriptedResponse::json(
            &HostedWireCase::anthropic(vec![json!({"type":"text","text":"continued"})], "end_turn")
                .to_string(),
        ),
    ])
    .await;
    let provider = WireProvider::Anthropic.build(server.base_url());
    let model = WireProvider::Anthropic.model(&provider, "claude-sonnet-4-6");
    let first = model
        .generate(&WireRequest::user("Search IANA."))
        .await
        .unwrap();
    model
        .generate(&WireRequest::continuation(first.message, "Continue."))
        .await
        .unwrap();
    provider.shutdown().await.unwrap();
    let requests = server.requests();
    let replay: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(replay["messages"][0]["content"], json!(content));
}

#[tokio::test]
async fn anthropic_url_outcomes_retain_target_and_do_not_turn_timestamp_into_excerpt() {
    for streaming in [false, true] {
        let native = HostedWireCase::anthropic(
            vec![
                json!({"type":"server_tool_use","id":"srv_fetch","name":"web_fetch","input":{"url":HostedWireCase::URL}}),
                json!({"type":"web_fetch_tool_result","tool_use_id":"srv_fetch","content":{"type":"web_fetch_result","url":HostedWireCase::URL,"retrieved_at":"2026-09-22T00:00:00Z","content":{"type":"document","source":{"type":"text","media_type":"text/plain","data":"IANA example domains"},"title":"Example domains"}}}),
            ],
            "end_turn",
        );
        let response = HostedWireCase::new(WireProvider::Anthropic, HostedTool::UrlContext, native)
            .generate(streaming)
            .await
            .unwrap();
        assert_eq!(response.hosted_tool_outputs.len(), 1);
        let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
        assert_eq!(output["requested_url"], HostedWireCase::URL);
        assert_eq!(output["retrieved_url"], HostedWireCase::URL);
        assert_eq!(output["status"]["state"], "completed");
        for citation in output["citations"].as_array().unwrap() {
            assert_ne!(citation["snippet"], "2026-09-22T00:00:00Z");
        }
    }
}

#[tokio::test]
async fn anthropic_url_errors_are_accepted_classified_outcomes_for_the_actual_target() {
    let mut failures = Vec::new();
    for (code, expected) in [
        ("url_not_accessible", ErrorKind::ProviderRejected),
        ("invalid_tool_input", ErrorKind::InvalidRequest),
        ("url_not_allowed", ErrorKind::PermissionDenied),
        ("too_many_requests", ErrorKind::RateLimited),
        ("max_uses_exceeded", ErrorKind::ResourceExhausted),
        ("unavailable", ErrorKind::ServiceUnavailable),
        ("future_failure", ErrorKind::ProviderRejected),
    ] {
        for streaming in [false, true] {
            let native = HostedWireCase::anthropic(
                vec![
                    json!({"type":"server_tool_use","id":"srv_fetch_failed","name":"web_fetch","input":{"url":"https://owned.invalid/fixture"}}),
                    json!({"type":"web_fetch_tool_result","tool_use_id":"srv_fetch_failed","content":{"type":"web_fetch_tool_result_error","error_code":code}}),
                ],
                "end_turn",
            );
            let fixture =
                HostedWireCase::new(WireProvider::Anthropic, HostedTool::UrlContext, native);
            let server = spawn_scripted_http_server(vec![fixture.reply(streaming)]).await;
            let provider = WireProvider::Anthropic.build(server.base_url());
            let model = WireProvider::Anthropic.model(&provider, "claude-sonnet-4-6");
            let request = WireRequest::user("Fetch the configured URL.")
                .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::UrlContext)])
                .unwrap();
            let observed = tokio::time::timeout(Duration::from_secs(10), async {
                if streaming {
                    let mut stream = model.generate_stream(&request).await?;
                    let mut response = None;
                    let mut completed = 0;
                    while let Some(event) = stream.next().await {
                        if let GenerateEvent::Completed(value) = event? {
                            completed += 1;
                            response = Some(*value);
                        }
                    }
                    let eof = stream.next().await.is_none();
                    Ok::<_, SdkError>((response, completed, eof))
                } else {
                    model
                        .generate(&request)
                        .await
                        .map(|response| (Some(response), 1, true))
                }
            })
            .await;
            let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
            let outcome = match &observed {
                Ok(Ok((Some(response), completed, eof))) => {
                    let output = match response.hosted_tool_outputs.as_slice() {
                        [
                            HostedToolOutput::UrlContext {
                                requested_url,
                                retrieved_url,
                                status: Some(HostedToolStatus::Failed { error }),
                                citations,
                            },
                        ] => json!({
                            "kind":format!("{:?}", error.kind()),
                            "code":error.native_details().code,
                            "error_type":error.native_details().error_type,
                            "body_contains_code":error.native_details().body.as_deref()
                                .is_some_and(|body| body.contains(code)),
                            "tool_call_id":error.native_details().tool_call_id,
                            "requested_url":requested_url, "retrieved_url":retrieved_url,
                            "citations":citations.len()
                        }),
                        other => json!({"unexpected_outputs":other}),
                    };
                    json!({
                        "model":response.model.as_str(),
                        "finish_reason":format!("{:?}", response.finish_reason),
                        "completed":completed, "eof":eof,
                        "hosted_outputs":response.hosted_tool_outputs.len(),
                        "global_citations":response.citations.len(), "output":output
                    })
                }
                other => json!({"operation_failure":format!("{other:?}")}),
            };
            let actual = json!({
                "outcome":outcome, "shutdown":format!("{shutdown:?}"),
                "requests":server.requests().iter().map(|request| {
                    json!({"method":request.method,"path":request.path})
                }).collect::<Vec<_>>()
            });
            let expected_outcome = json!({
                "outcome":{
                    "model":"claude-sonnet-4-6", "finish_reason":"Some(Stop)",
                    "completed":1, "eof":true, "hosted_outputs":1, "global_citations":0,
                    "output":{
                        "kind":format!("{expected:?}"), "code":code,
                        "error_type":"web_fetch_tool_result_error", "body_contains_code":true,
                        "tool_call_id":"srv_fetch_failed",
                        "requested_url":"https://owned.invalid/fixture", "retrieved_url":null,
                        "citations":0
                    }
                },
                "shutdown":"Ok(Ok(()))", "requests":[{"method":"POST","path":"/v1/messages"}]
            });
            let evidence = json!({
                "test":"anthropic_url_errors_are_accepted_classified_outcomes_for_the_actual_target",
                "code":code, "streaming":streaming,
                "expected":expected_outcome, "actual":actual,
                "shutdown_passed":matches!(shutdown, Ok(Ok(()))),
                "passed":actual == expected_outcome
            });
            println!("REGRESSION_CASE {evidence}");
            if actual != expected_outcome {
                failures.push(evidence);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "hosted fetch fixtures failed: {failures:?}"
    );
}

#[tokio::test]
async fn anthropic_code_preserves_process_failure_and_matching_command() {
    for (exit, expected) in [(0, "completed"), (7, "failed")] {
        for streaming in [false, true] {
            let native = HostedWireCase::anthropic(
                vec![
                    json!({"type":"server_tool_use","id":"srv_code","name":"bash_code_execution","input":{"command":"python3 -c 'print(1)'"}}),
                    json!({"type":"bash_code_execution_tool_result","tool_use_id":"srv_code","content":{"type":"bash_code_execution_result","stdout":"native stdout","stderr":"native stderr","return_code":exit,"content":[]}}),
                ],
                "end_turn",
            );
            let response =
                HostedWireCase::new(WireProvider::Anthropic, HostedTool::CodeExecution, native)
                    .generate(streaming)
                    .await
                    .unwrap();
            assert_eq!(response.hosted_tool_outputs.len(), 1);
            let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
            assert_eq!(output["code"][0]["code"], "python3 -c 'print(1)'");
            assert_eq!(output["status"]["state"], expected);
            assert_eq!(output["results"][0]["status"]["state"], expected);
            assert_eq!(output["results"][0]["stdout"], "native stdout");
            assert_eq!(output["results"][0]["stderr"], "native stderr");
            assert_eq!(output["results"][0]["logs"], Value::Null);
            assert_eq!(output["results"][0]["exit_code"], exit);
            if exit != 0 {
                assert_eq!(
                    output["results"][0]["status"]["error"]["details"]["execution_exit_code"],
                    exit
                );
            }
        }
    }
}

#[tokio::test]
async fn gemini_grounding_keeps_native_redirect_and_has_no_execution_status() {
    let redirect = "https://vertexaisearch.cloud.google.com/grounding-api-redirect/native-token";
    let mut native = HostedWireCase::gemini(vec![json!({"text":"IANA"})]);
    native["candidates"][0]["groundingMetadata"] = json!({
        "webSearchQueries":["IANA example domains"],
        "groundingChunks":[{"web":{"uri":redirect,"title":"Example Domains"}}],
        "groundingSupports":[{"segment":{"startIndex":0,"endIndex":4,"text":"IANA"},"groundingChunkIndices":[0]}]
    });
    for streaming in [false, true] {
        let response =
            HostedWireCase::new(WireProvider::Gemini, HostedTool::WebSearch, native.clone())
                .generate(streaming)
                .await
                .unwrap();
        let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
        assert_eq!(output["status"], Value::Null);
        assert_eq!(output["action"]["type"], "search");
        assert_eq!(output["results"][0]["url"], redirect);
        assert_eq!(output["citations"][0]["url"], redirect);
        assert!(
            !serde_json::to_string(&response.citations)
                .unwrap()
                .contains(HostedWireCase::URL)
        );
    }
}

#[tokio::test]
async fn gemini_url_failure_unary_preserves_failure_but_rejects_dangling_grounding() {
    let url = "https://httpbin.org/status/404?strict_live=wire-fixture";
    for dangling in [true, false] {
        let mut native = HostedWireCase::gemini(vec![json!({"text":"RETRIEVAL_FAILED"})]);
        native["candidates"][0]["urlContextMetadata"] = json!({"urlMetadata":[{
            "retrievedUrl":url,"urlRetrievalStatus":"URL_RETRIEVAL_STATUS_ERROR"
        }]});
        if dangling {
            native["candidates"][0]["groundingMetadata"] = json!({"groundingSupports":[{
                "segment":{"startIndex":0,"endIndex":16,"text":"RETRIEVAL_FAILED"},"groundingChunkIndices":[0]
            }]});
        }
        let result = HostedWireCase::new(WireProvider::Gemini, HostedTool::UrlContext, native)
            .generate(false)
            .await;
        if dangling {
            let error = result.unwrap_err();
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
            let response = result.unwrap();
            assert_eq!(response.finish_reason, Some(FinishReason::Stop));
            let [
                HostedToolOutput::UrlContext {
                    retrieved_url,
                    status: Some(HostedToolStatus::Failed { error }),
                    citations,
                    ..
                },
            ] = response.hosted_tool_outputs.as_slice()
            else {
                panic!("one native URL failure must survive normalization")
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
async fn gemini_url_context_has_one_honest_outcome_per_native_target() {
    let mut native = HostedWireCase::gemini(vec![
        json!({"text":"Retrieved one URL and failed another."}),
    ]);
    native["candidates"][0]["urlContextMetadata"] = json!({"urlMetadata":[
        {"retrievedUrl":HostedWireCase::URL,"urlRetrievalStatus":"URL_RETRIEVAL_STATUS_SUCCESS"},
        {"retrievedUrl":"https://owned.invalid/fixture","urlRetrievalStatus":"URL_RETRIEVAL_STATUS_ERROR"}
    ]});
    for streaming in [false, true] {
        let response =
            HostedWireCase::new(WireProvider::Gemini, HostedTool::UrlContext, native.clone())
                .generate(streaming)
                .await
                .unwrap();
        assert_eq!(response.hosted_tool_outputs.len(), 2);
        let outputs = serde_json::to_value(&response.hosted_tool_outputs).unwrap();
        assert_eq!(outputs[0]["requested_url"], Value::Null);
        assert_eq!(outputs[0]["retrieved_url"], HostedWireCase::URL);
        assert_eq!(outputs[0]["status"]["state"], "completed");
        assert_eq!(outputs[1]["retrieved_url"], "https://owned.invalid/fixture");
        assert_eq!(outputs[1]["status"]["state"], "failed");
        assert_eq!(
            outputs[1]["status"]["error"]["details"]["outcome"],
            "URL_RETRIEVAL_STATUS_ERROR"
        );
        for output in outputs.as_array().unwrap() {
            for citation in output["citations"].as_array().unwrap() {
                assert!(citation["snippet"].is_null());
            }
        }
    }
}

#[tokio::test]
async fn gemini_url_failure_categories_are_not_interchangeable() {
    for (outcome, expected) in [
        ("URL_RETRIEVAL_STATUS_ERROR", ErrorKind::ProviderRejected),
        ("URL_RETRIEVAL_STATUS_PAYWALL", ErrorKind::PermissionDenied),
        ("URL_RETRIEVAL_STATUS_UNSAFE", ErrorKind::PermissionDenied),
    ] {
        let mut native = HostedWireCase::gemini(vec![json!({"text":"Retrieval did not succeed."})]);
        native["candidates"][0]["urlContextMetadata"] = json!({"urlMetadata":[{"retrievedUrl":HostedWireCase::URL,"urlRetrievalStatus":outcome}]});
        let response = HostedWireCase::new(WireProvider::Gemini, HostedTool::UrlContext, native)
            .generate(false)
            .await
            .unwrap();
        let HostedToolOutput::UrlContext {
            status: Some(HostedToolStatus::Failed { error }),
            ..
        } = &response.hosted_tool_outputs[0]
        else {
            panic!("native failure was lost");
        };
        assert_eq!(error.kind(), expected);
        assert_eq!(
            error.classification_basis(),
            ClassificationBasis::NativeOutcome
        );
        assert_eq!(error.native_details().outcome.as_deref(), Some(outcome));
        assert_eq!(error.native_details().code, None);
    }
}

#[tokio::test]
async fn gemini_unknown_url_outcome_cannot_be_normalized_as_success() {
    let mut native = HostedWireCase::gemini(vec![json!({"text":"Unknown result."})]);
    native["candidates"][0]["urlContextMetadata"] = json!({"urlMetadata":[{"retrievedUrl":HostedWireCase::URL,"urlRetrievalStatus":"URL_RETRIEVAL_STATUS_FUTURE"}]});
    let error = HostedWireCase::new(WireProvider::Gemini, HostedTool::UrlContext, native)
        .generate(false)
        .await
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::InvalidResponse(ResponseViolation::UnknownVariant { .. })
    ));
}

#[tokio::test]
async fn gemini_code_pairs_native_ids_without_inventing_lifecycle_or_exit() {
    for include_id in [false, true] {
        for streaming in [false, true] {
            let mut code = json!({"language":"PYTHON","code":"print(338350)"});
            let mut result = json!({"outcome":"OUTCOME_OK","output":"338350\n"});
            if include_id {
                code["id"] = json!("execution_one");
                result["id"] = json!("execution_one");
            }
            let native = HostedWireCase::gemini(vec![
                json!({"executableCode":code}),
                json!({"codeExecutionResult":result}),
                json!({"text":"338350"}),
            ]);
            let response =
                HostedWireCase::new(WireProvider::Gemini, HostedTool::CodeExecution, native)
                    .generate(streaming)
                    .await
                    .unwrap();
            assert_eq!(response.hosted_tool_outputs.len(), 1);
            let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
            assert_eq!(output["status"], Value::Null);
            assert_eq!(output["code"][0]["code"], "print(338350)");
            assert_eq!(output["results"][0]["status"]["state"], "completed");
            assert_eq!(output["results"][0]["stdout"], "338350\n");
            assert_eq!(output["results"][0]["logs"], Value::Null);
            assert_eq!(output["results"][0]["exit_code"], Value::Null);
        }
    }
}

#[tokio::test]
async fn gemini_code_failures_preserve_native_outcome_and_unclassified_output() {
    let source =
        "raise RuntimeError(\"STRICT_HOSTED_FAILURE_strict-84d5aa446f07484db832021395fda498\")\n";
    let logs = "Traceback (most recent call last):\n  File \"<string>\", line 1, in <module>\nRuntimeError: STRICT_HOSTED_FAILURE_strict-84d5aa446f07484db832021395fda498\n";
    for streaming in [false, true] {
        for native_id in [None, Some("execution_error")] {
            for (outcome, expected) in [
                ("OUTCOME_FAILED", ErrorKind::ProviderRejected),
                ("OUTCOME_DEADLINE_EXCEEDED", ErrorKind::DeadlineExceeded),
            ] {
                let mut code = json!({"language":"PYTHON","code":source});
                let mut result = json!({"outcome":outcome,"output":logs});
                if let Some(id) = native_id {
                    code["id"] = json!(id);
                    result["id"] = json!(id);
                }
                let native = HostedWireCase::gemini(vec![
                    json!({"executableCode":code}),
                    json!({"codeExecutionResult":result}),
                    json!({"text":"Execution failed."}),
                ]);
                let response =
                    HostedWireCase::new(WireProvider::Gemini, HostedTool::CodeExecution, native)
                        .generate(streaming)
                        .await
                        .unwrap();
                assert_eq!(response.hosted_tool_outputs.len(), 1);
                let HostedToolOutput::CodeExecution {
                    status,
                    code,
                    results,
                    ..
                } = &response.hosted_tool_outputs[0]
                else {
                    panic!("code output");
                };
                assert_eq!(status, &None);
                assert_eq!(code.len(), 1);
                assert_eq!(code[0].language.as_deref(), Some("PYTHON"));
                assert_eq!(code[0].code, source);
                assert_eq!(results.len(), 1);
                let Some(HostedToolStatus::Failed { error }) = &results[0].status else {
                    panic!("native execution failure");
                };
                assert_eq!(error.kind(), expected);
                assert_eq!(
                    error.classification_basis(),
                    ClassificationBasis::NativeOutcome
                );
                assert_eq!(error.native_details().outcome.as_deref(), Some(outcome));
                assert_eq!(error.native_details().code, None);
                let result = serde_json::to_value(&results[0]).unwrap();
                assert_eq!(result["logs"], logs);
                assert_eq!(result["stdout"], Value::Null);
                assert_eq!(result["stderr"], Value::Null);
                assert_eq!(result["exit_code"], Value::Null);
            }
        }
    }
}

#[tokio::test]
async fn gemini_code_duplicate_native_links_are_rejected() {
    for parts in [
        vec![
            json!({"executableCode":{"id":"one","language":"PYTHON","code":"print(1)"}}),
            json!({"executableCode":{"id":"one","language":"PYTHON","code":"print(2)"}}),
        ],
        vec![
            json!({"executableCode":{"id":"one","language":"PYTHON","code":"print(1)"}}),
            json!({"codeExecutionResult":{"id":"one","outcome":"OUTCOME_OK","output":"1"}}),
            json!({"codeExecutionResult":{"id":"one","outcome":"OUTCOME_OK","output":"1"}}),
        ],
    ] {
        let error = HostedWireCase::new(
            WireProvider::Gemini,
            HostedTool::CodeExecution,
            HostedWireCase::gemini(parts),
        )
        .generate(false)
        .await
        .unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::InvalidResponse(_)));
    }
}

#[tokio::test]
async fn gemini_code_uses_native_ids_instead_of_result_arrival_order() {
    let native = HostedWireCase::gemini(vec![
        json!({"executableCode":{"id":"one","language":"PYTHON","code":"print(1)"}}),
        json!({"executableCode":{"id":"two","language":"PYTHON","code":"print(2)"}}),
        json!({"codeExecutionResult":{"id":"two","outcome":"OUTCOME_OK","output":"2"}}),
        json!({"codeExecutionResult":{"id":"one","outcome":"OUTCOME_OK","output":"1"}}),
    ]);
    let response = HostedWireCase::new(WireProvider::Gemini, HostedTool::CodeExecution, native)
        .generate(false)
        .await
        .unwrap();
    assert_eq!(response.hosted_tool_outputs.len(), 2);
    let outputs = serde_json::to_value(&response.hosted_tool_outputs).unwrap();
    for (index, value) in ["1", "2"].into_iter().enumerate() {
        assert_eq!(outputs[index]["code"][0]["code"], format!("print({value})"));
        assert_eq!(outputs[index]["results"][0]["stdout"], value);
    }
}

#[tokio::test]
async fn gemini_unmatched_or_ambiguous_results_are_retained_without_invented_association() {
    for parts in [
        vec![
            json!({"executableCode":{"id":"one","language":"PYTHON","code":"print(1)"}}),
            json!({"codeExecutionResult":{"id":"prior_turn","outcome":"OUTCOME_OK","output":"other"}}),
        ],
        vec![
            json!({"executableCode":{"language":"PYTHON","code":"print(1)"}}),
            json!({"executableCode":{"language":"PYTHON","code":"print(2)"}}),
            json!({"codeExecutionResult":{"outcome":"OUTCOME_OK","output":"ambiguous"}}),
        ],
    ] {
        let expected_records = parts.len();
        let response = HostedWireCase::new(
            WireProvider::Gemini,
            HostedTool::CodeExecution,
            HostedWireCase::gemini(parts),
        )
        .generate(false)
        .await
        .unwrap();
        assert_eq!(response.hosted_tool_outputs.len(), expected_records);
        for output in &response.hosted_tool_outputs {
            let HostedToolOutput::CodeExecution {
                status,
                code,
                results,
                ..
            } = output
            else {
                panic!("native code part");
            };
            assert_eq!(status, &None);
            assert!(
                code.is_empty() || results.is_empty(),
                "unrelated parts must not become one correlated operation"
            );
        }
    }
}

#[tokio::test]
async fn gemini_thought_marker_does_not_hide_native_code_execution_parts() {
    let native = HostedWireCase::gemini(vec![
        json!({"thought":true,"thoughtSignature":"opaque-code-signature","executableCode":{"id":"thought_code","language":"PYTHON","code":"print(1)"}}),
        json!({"thought":true,"codeExecutionResult":{"id":"thought_code","outcome":"OUTCOME_OK","output":"1"}}),
        json!({"text":"1"}),
    ]);
    for streaming in [false, true] {
        let response = HostedWireCase::new(
            WireProvider::Gemini,
            HostedTool::CodeExecution,
            native.clone(),
        )
        .generate(streaming)
        .await
        .unwrap();
        assert_eq!(response.hosted_tool_outputs.len(), 1);
        let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
        assert_eq!(output["code"][0]["code"], "print(1)");
        assert_eq!(output["results"][0]["stdout"], "1");
    }
}

#[tokio::test]
async fn gemini_stream_grounding_references_accumulated_chunks_with_global_indices() {
    let redirect =
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/native-stream-token";
    let first = json!({"modelVersion":"models/gemini-2.5-flash","candidates":[{
        "content":{"role":"model","parts":[{"text":"IA"}]},
        "groundingMetadata":{"webSearchQueries":["IANA example domains"],"groundingChunks":[{"web":{"uri":redirect,"title":"Example Domains"}}]}
    }]});
    let second = json!({"modelVersion":"models/gemini-2.5-flash","candidates":[{
        "content":{"role":"model","parts":[{"text":"NA"}]},"finishReason":"STOP",
        "groundingMetadata":{"groundingSupports":[{"segment":{"startIndex":0,"endIndex":4,"text":"IANA"},"groundingChunkIndices":[0]}]}
    }]});
    let server = spawn_scripted_http_server(vec![ScriptedResponse::sse(&format!(
        "data: {first}\n\ndata: {second}\n\n"
    ))])
    .await;
    let provider = WireProvider::Gemini.build(server.base_url());
    let model = WireProvider::Gemini.model(&provider, "models/gemini-2.5-flash");
    let mut events = model
        .generate_stream(
            &WireRequest::user("Search IANA.")
                .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::WebSearch)])
                .unwrap(),
        )
        .await
        .unwrap();
    let response = loop {
        match events.next().await {
            Some(Ok(GenerateEvent::Completed(response))) => break response,
            Some(Ok(_)) => {}
            Some(Err(error)) => panic!("global grounding reference must normalize: {error}"),
            None => panic!("missing completion"),
        }
    };
    assert!(events.next().await.is_none());
    provider.shutdown().await.unwrap();
    let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
    assert_eq!(output["results"][0]["url"], redirect);
    assert_eq!(output["citations"][0]["url"], redirect);
    assert_eq!(output["citations"][0]["snippet"], "IANA");
    assert_eq!(output["citations"][0]["start_index"], 0);
    assert_eq!(output["citations"][0]["end_index"], 4);
}

#[tokio::test]
async fn anthropic_process_results_require_native_process_fields() {
    for (tool, outer, inner) in [
        (
            "code_execution",
            "code_execution_tool_result",
            "code_execution_result",
        ),
        (
            "bash_code_execution",
            "bash_code_execution_tool_result",
            "bash_code_execution_result",
        ),
    ] {
        for field in ["stdout", "stderr", "return_code"] {
            for replacement in [None, Some(json!({"invalid":"shape"}))] {
                let mut result = json!({"type":inner,"stdout":"native stdout","stderr":"native stderr","return_code":0,"content":[]});
                if let Some(value) = replacement {
                    result[field] = value;
                } else {
                    result.as_object_mut().unwrap().remove(field);
                }
                let native = HostedWireCase::anthropic(
                    vec![
                        json!({"type":"server_tool_use","id":"srv_required","name":tool,"input":{"code":"print(1)","command":"python3 -c 'print(1)'"}}),
                        json!({"type":outer,"tool_use_id":"srv_required","content":result}),
                    ],
                    "end_turn",
                );
                let error =
                    HostedWireCase::new(WireProvider::Anthropic, HostedTool::CodeExecution, native)
                        .generate(false)
                        .await
                        .unwrap_err();
                assert!(
                    matches!(error.kind(), ErrorKind::InvalidResponse(ResponseViolation::MissingField { field: failed } | ResponseViolation::InvalidField { field: failed }) if failed == field)
                );
            }
        }
    }
}

#[tokio::test]
async fn gemini_web_grounding_retains_sources_without_inventing_search_action() {
    let redirect =
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/native-optional-query";
    let mut native = HostedWireCase::gemini(vec![json!({"text":"IANA"})]);
    native["candidates"][0]["groundingMetadata"] = json!({
        "groundingChunks":[{"web":{"uri":redirect,"title":"Example Domains"}}],
        "groundingSupports":[{"segment":{"startIndex":0,"endIndex":4,"text":"IANA"},"groundingChunkIndices":[0]}]
    });
    for entry_point in [
        None,
        Some(json!({"renderedContent":"<div>Search suggestions</div>"})),
    ] {
        let mut native = native.clone();
        if let Some(entry_point) = entry_point {
            native["candidates"][0]["groundingMetadata"]["searchEntryPoint"] = entry_point;
        }
        for streaming in [false, true] {
            let response =
                HostedWireCase::new(WireProvider::Gemini, HostedTool::WebSearch, native.clone())
                    .generate(streaming)
                    .await
                    .unwrap();
            let output = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
            assert_eq!(
                output["action"],
                Value::Null,
                "source chunks and search-entry presentation do not report an executed query"
            );
            assert_eq!(output["status"], Value::Null);
            assert_eq!(output["results"][0]["url"], redirect);
            assert_eq!(output["citations"][0]["url"], redirect);
        }
    }
}

#[tokio::test]
async fn gemini_url_grounding_preserves_retrieval_without_inventing_search_action() {
    let mut native = HostedWireCase::gemini(vec![json!({"text":"IANA"})]);
    native["candidates"][0]["groundingMetadata"] = json!({
        "groundingChunks":[{"web":{"uri":HostedWireCase::URL,"title":"Example Domains"}}],
        "groundingSupports":[{"segment":{"startIndex":0,"endIndex":4,"text":"IANA"},"groundingChunkIndices":[0]}]
    });
    native["candidates"][0]["urlContextMetadata"] = json!({"urlMetadata":[{
        "retrievedUrl":HostedWireCase::URL,"urlRetrievalStatus":"URL_RETRIEVAL_STATUS_SUCCESS"
    }]});
    for streaming in [false, true] {
        let response =
            HostedWireCase::new(WireProvider::Gemini, HostedTool::UrlContext, native.clone())
                .generate(streaming)
                .await
                .unwrap();
        assert_eq!(response.finish_reason, Some(FinishReason::Stop));
        assert_eq!(response.hosted_tool_outputs.len(), 2);
        let web = serde_json::to_value(&response.hosted_tool_outputs[0]).unwrap();
        assert_eq!(web["family"], "web_search");
        assert_eq!(
            web["action"],
            Value::Null,
            "web grounding alongside native URL retrieval does not report a search invocation"
        );
        assert_eq!(web["status"], Value::Null);
        assert_eq!(web["results"][0]["url"], HostedWireCase::URL);
        assert_eq!(web["citations"][0]["url"], HostedWireCase::URL);
        let url = serde_json::to_value(&response.hosted_tool_outputs[1]).unwrap();
        assert_eq!(url["family"], "url_context");
        assert_eq!(url["retrieved_url"], HostedWireCase::URL);
        assert_eq!(url["status"]["state"], "completed");
        assert_eq!(url["citations"][0]["url"], HostedWireCase::URL);
    }
}

#[tokio::test]
async fn anthropic_hosted_invocation_name_keeps_the_observed_discriminator() {
    for name in [None, Some("future_hosted_tool")] {
        let mut invocation = json!({"type":"server_tool_use","id":"srv_unknown","input":{}});
        if let Some(name) = name {
            invocation["name"] = json!(name);
        }
        let error = HostedWireCase::new(
            WireProvider::Anthropic,
            HostedTool::WebSearch,
            HostedWireCase::anthropic(vec![invocation], "end_turn"),
        )
        .generate(false)
        .await
        .unwrap_err();
        match name {
            None => assert!(matches!(
                error.kind(),
                ErrorKind::InvalidResponse(ResponseViolation::MissingField { field: "name" })
            )),
            Some(expected) => {
                assert!(matches!(
                    error.kind(),
                    ErrorKind::InvalidResponse(ResponseViolation::UnknownVariant { field: "name" })
                ));
                assert!(
                    matches!(error.response_details(), Some(ResponseDetails::Discriminator { actual }) if actual == expected)
                );
            }
        }
    }
}

#[tokio::test]
async fn openai_failed_response_retains_outer_stream_failure_evidence() {
    let payload = json!({
        "type":"response.failed", "sequence_number":42,
        "response":{"id":"resp_failed","model":"gpt-4.1","status":"failed","output":[],
            "error":{"code":"model_not_found","message":"exact unavailable model"}}
    });
    let error = WireProvider::OpenAi
        .stream_failure(
            ScriptedResponse::sse(&format!(
                "event: response.failed\nid: native-failed-event\ndata: {payload}\n\n"
            ))
            .with_header("x-request-id", "native-failed-request")
            .with_header("retry-after", "7"),
        )
        .await;
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert_eq!(
        error.classification_basis(),
        ClassificationBasis::NativeCode
    );
    assert_eq!(error.http_status(), None);
    assert_eq!(
        error.native_details().event_type.as_deref(),
        Some("response.failed")
    );
    assert_eq!(error.native_details().sequence_number, Some(42));
    assert_eq!(
        error.native_details().event_id.as_deref(),
        Some("native-failed-event")
    );
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("native-failed-request")
    );
    assert_eq!(error.native_details().retry_after.as_deref(), Some("7"));
    assert_eq!(
        serde_json::from_str::<Value>(error.native_details().body.as_deref().unwrap()).unwrap(),
        payload
    );
    assert!(!format!("{error:?}").contains("exact unavailable model"));
}
