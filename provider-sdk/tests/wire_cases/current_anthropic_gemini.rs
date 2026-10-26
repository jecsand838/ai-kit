use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use futures_util::StreamExt;
use provider_sdk::batch::BatchResult;
use provider_sdk::error::{ErrorKind, ErrorPhase};
use provider_sdk::generation::{
    ContentPart, FinishReason, FunctionCall, GenerateEvent, GenerationConfig, JsonSchema, Message,
    ProviderFunctionTool, ProviderToolSpec, ReasoningConfig, ReasoningEffort, ReasoningSummaryMode,
    ToolChoice,
};
use provider_sdk::hosted::HostedTool;
use provider_sdk::{ModelBackend, ProviderBackend};
use serde_json::{Value, json};

use crate::support::scripted_http::{ScriptedResponse, spawn_scripted_http_server};
use crate::{WireProvider, WireRequest, WireResponse};

#[tokio::test]
async fn current_claude_profiles_render_effort_without_overriding_summary_defaults() {
    for model_id in [
        "claude-fable-5-1",
        "claude-opus-5-5",
        "claude-sonnet-5-5",
        "claude-haiku-5-5",
    ] {
        for (effort, expected) in [
            (ReasoningEffort::Minimal, "low"),
            (ReasoningEffort::Low, "low"),
            (ReasoningEffort::Medium, "medium"),
            (ReasoningEffort::High, "high"),
            (ReasoningEffort::Maximum, "max"),
        ] {
            let mut server = spawn_scripted_http_server(vec![WireResponse::anthropic_text(
                "msg", model_id, "done",
            )])
            .await;
            let provider = WireProvider::Anthropic.build(server.base_url());
            let observed = WireProvider::Anthropic
                .model(&provider, model_id)
                .generate(&WireRequest::user("probe").with_reasoning(ReasoningConfig {
                    effort: Some(effort),
                    ..ReasoningConfig::default()
                }))
                .await;
            let shutdown = provider.shutdown().await;
            let settled = server.finish(Duration::from_secs(1)).await;
            assert!(
                observed.is_ok(),
                "{model_id}/{effort:?}: {observed:?}; cleanup={shutdown:?}/{settled:?}"
            );
            shutdown.unwrap();
            settled.unwrap();
            let requests = server.requests();
            let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
            assert_eq!(body["thinking"], json!({"type":"adaptive"}));
            assert_eq!(body["output_config"]["effort"], expected);
        }
    }
}

#[tokio::test]
async fn current_claude_forced_tools_follow_model_and_thinking_mode() {
    for (model_id, effort, budget, accepted) in [
        ("claude-opus-5-5", None, None, false),
        ("claude-sonnet-5-5", None, None, false),
        ("claude-fable-5-1", None, None, false),
        ("claude-haiku-5-5", Some(ReasoningEffort::Low), None, true),
        ("claude-opus-5", Some(ReasoningEffort::Low), None, true),
        ("claude-sonnet-4-6", None, Some(1024), false),
    ] {
        let mut server = spawn_scripted_http_server(if accepted {
            vec![WireResponse::anthropic_text("msg", model_id, "done")]
        } else {
            Vec::new()
        })
        .await;
        let provider = WireProvider::Anthropic.build(server.base_url());
        let mut request = WireRequest::user("use lookup")
            .with_tools(vec![ProviderToolSpec::Function(
                ProviderFunctionTool::new(
                    "lookup",
                    None,
                    JsonSchema::new(json!({"type":"object","properties":{}})).unwrap(),
                )
                .unwrap(),
            )])
            .unwrap()
            .with_generation(
                GenerationConfig::default()
                    .with_max_output_tokens(NonZeroU32::new(2048).unwrap())
                    .with_tool_choice(ToolChoice::Required),
            )
            .unwrap();
        if effort.is_some() || budget.is_some() {
            request = request.with_reasoning(ReasoningConfig {
                effort,
                max_reasoning_tokens: budget,
                summary_mode: ReasoningSummaryMode::Auto,
            });
        }
        let observed = WireProvider::Anthropic
            .model(&provider, model_id)
            .generate(&request)
            .await;
        let shutdown = provider.shutdown().await;
        let settled = server.finish(Duration::from_secs(1)).await;
        shutdown.unwrap();
        settled.unwrap();
        if accepted {
            assert!(observed.is_ok(), "{model_id}: {observed:?}");
            assert_eq!(server.requests().len(), 1);
        } else {
            assert!(
                matches!(observed, Err(ref e) if matches!(e.kind(), ErrorKind::Unsupported | ErrorKind::InvalidInput)),
                "{model_id}: {observed:?}"
            );
            assert!(server.requests().is_empty());
        }
    }
}

#[tokio::test]
async fn current_claude_rejects_unhonored_sampling_and_small_manual_budget_before_io() {
    for model_id in [
        "claude-fable-5-1",
        "claude-opus-5-5",
        "claude-sonnet-5-5",
        "claude-haiku-5-5",
    ] {
        for generation in [
            GenerationConfig::default().with_temperature(0.4).unwrap(),
            GenerationConfig::default().with_top_p(0.8).unwrap(),
            GenerationConfig::default().with_top_k(NonZeroU32::new(10).unwrap()),
        ] {
            let mut server = spawn_scripted_http_server(Vec::new()).await;
            let provider = WireProvider::Anthropic.build(server.base_url());
            let observed = WireProvider::Anthropic
                .model(&provider, model_id)
                .generate(
                    &WireRequest::user("probe")
                        .with_generation(generation)
                        .unwrap(),
                )
                .await;
            provider.shutdown().await.unwrap();
            server.finish(Duration::from_secs(1)).await.unwrap();
            assert!(
                matches!(observed, Err(ref e) if e.kind() == ErrorKind::Unsupported),
                "{model_id}: {observed:?}"
            );
            assert!(server.requests().is_empty());
        }
    }
    let mut server = spawn_scripted_http_server(Vec::new()).await;
    let provider = WireProvider::Anthropic.build(server.base_url());
    let observed = WireProvider::Anthropic
        .model(&provider, "claude-haiku-4-5")
        .generate(&WireRequest::user("probe").with_reasoning(ReasoningConfig {
            max_reasoning_tokens: Some(1023),
            ..ReasoningConfig::default()
        }))
        .await;
    provider.shutdown().await.unwrap();
    server.finish(Duration::from_secs(1)).await.unwrap();
    assert!(
        matches!(observed, Err(ref e) if e.kind() == ErrorKind::InvalidInput),
        "{observed:?}"
    );
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn current_gemini_effort_extremes_and_medium_use_supported_levels() {
    for model_id in [
        "models/gemini-4-argon",
        "models/gemini-3.8-flash",
        "models/gemini-3.1-pro-preview",
    ] {
        for (effort, expected) in [
            (ReasoningEffort::Minimal, "low"),
            (ReasoningEffort::Low, "low"),
            (ReasoningEffort::Medium, "medium"),
            (ReasoningEffort::High, "high"),
            (ReasoningEffort::Maximum, "high"),
        ] {
            let reply = json!({"modelVersion":model_id,"candidates":[{"content":{"parts":[{"text":"done"}]},"finishReason":"STOP"}]});
            let mut server =
                spawn_scripted_http_server(vec![ScriptedResponse::json(&reply.to_string())]).await;
            let provider = WireProvider::Gemini.build(server.base_url());
            let observed = WireProvider::Gemini
                .model(&provider, model_id)
                .generate(&WireRequest::user("probe").with_reasoning(ReasoningConfig {
                    effort: Some(effort),
                    ..ReasoningConfig::default()
                }))
                .await;
            let shutdown = provider.shutdown().await;
            let settled = server.finish(Duration::from_secs(1)).await;
            assert!(
                observed.is_ok(),
                "{model_id}/{effort:?}: {observed:?}; cleanup={shutdown:?}/{settled:?}"
            );
            shutdown.unwrap();
            settled.unwrap();
            let requests = server.requests();
            let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
            assert_eq!(
                body["generationConfig"]["thinkingConfig"],
                json!({"thinkingLevel":expected})
            );
        }
    }
}

#[tokio::test]
async fn current_gemini_rejects_ignored_sampling_before_io() {
    for model_id in ["models/gemini-4-argon", "models/gemini-3.8-flash"] {
        for generation in [
            GenerationConfig::default().with_temperature(0.4).unwrap(),
            GenerationConfig::default().with_top_p(0.8).unwrap(),
            GenerationConfig::default().with_top_k(NonZeroU32::new(10).unwrap()),
        ] {
            let mut server = spawn_scripted_http_server(Vec::new()).await;
            let provider = WireProvider::Gemini.build(server.base_url());
            let observed = WireProvider::Gemini
                .model(&provider, model_id)
                .generate(
                    &WireRequest::user("probe")
                        .with_generation(generation)
                        .unwrap(),
                )
                .await;
            provider.shutdown().await.unwrap();
            server.finish(Duration::from_secs(1)).await.unwrap();
            assert!(
                matches!(observed, Err(ref e) if e.kind() == ErrorKind::Unsupported),
                "{model_id}: {observed:?}"
            );
            assert!(server.requests().is_empty());
        }
    }
}

#[tokio::test]
async fn current_models_render_existing_hosted_declarations() {
    for (kind, model_id) in [
        (WireProvider::Anthropic, "claude-fable-5-1"),
        (WireProvider::Anthropic, "claude-opus-5-5"),
        (WireProvider::Anthropic, "claude-sonnet-5-5"),
        (WireProvider::Anthropic, "claude-haiku-5-5"),
        (WireProvider::Gemini, "models/gemini-4-argon"),
        (WireProvider::Gemini, "models/gemini-3.8-flash"),
    ] {
        let reply = match kind { WireProvider::Anthropic => WireResponse::anthropic_text("msg", model_id, "done"), _ => ScriptedResponse::json(&json!({"modelVersion":model_id,"candidates":[{"content":{"parts":[{"text":"done"}]},"finishReason":"STOP"}]}).to_string()) };
        let mut server = spawn_scripted_http_server(vec![reply]).await;
        let provider = kind.build(server.base_url());
        let observed = kind
            .model(&provider, model_id)
            .generate(
                &WireRequest::user("search")
                    .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::WebSearch)])
                    .unwrap(),
            )
            .await;
        let shutdown = provider.shutdown().await;
        let settled = server.finish(Duration::from_secs(1)).await;
        assert!(
            observed.is_ok(),
            "{model_id}: {observed:?}; cleanup={shutdown:?}/{settled:?}"
        );
        shutdown.unwrap();
        settled.unwrap();
        let requests = server.requests();
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["tools"].as_array().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn gemini_current_policy_terminals_allow_absent_output() {
    for (native, expected) in [
        ("ESCALATION", FinishReason::ContentFilter),
        ("PUP_LIMITED_DISABLED", FinishReason::Error),
    ] {
        for (streaming, partial) in [(false, false), (true, false), (true, true)] {
            let body =
                json!({"modelVersion":"gemini-3.8-flash","candidates":[{"finishReason":native}]})
                    .to_string();
            let reply = if streaming {
                let prefix = if partial {
                    format!(
                        "data: {}\n\n",
                        json!({"candidates":[{"content":{"role":"model","parts":[{"text":"partial"}]}}]})
                    )
                } else {
                    String::new()
                };
                ScriptedResponse::sse(&format!("{prefix}data: {body}\n\n"))
            } else {
                ScriptedResponse::json(&body)
            };
            let mut server = spawn_scripted_http_server(vec![reply]).await;
            let provider = WireProvider::Gemini.build(server.base_url());
            let model = WireProvider::Gemini.model(&provider, "models/gemini-3.8-flash");
            let request = WireRequest::user("probe");
            let observed = if streaming {
                let mut stream = model.generate_stream(&request).await.unwrap();
                let mut terminal = None;
                while let Some(event) = stream.next().await {
                    match event {
                        Ok(GenerateEvent::Completed(response)) => terminal = Some(Ok(*response)),
                        Err(error) => terminal = Some(Err(error)),
                        _ => {}
                    }
                }
                terminal.expect("terminal result")
            } else {
                model.generate(&request).await
            };
            provider.shutdown().await.unwrap();
            server.finish(Duration::from_secs(1)).await.unwrap();
            let response = observed.unwrap();
            assert_eq!(response.finish_reason, Some(expected.clone()));
            if partial {
                assert_eq!(
                    response.message.content(),
                    &[ContentPart::Text {
                        text: "partial".into()
                    }]
                );
            }
        }
    }
}

#[tokio::test]
async fn gemini_long_decode_never_completes_or_sends_a_hidden_continuation() {
    for streaming in [false, true] {
        let terminal = json!({"modelVersion":"gemini-4-argon","candidates":[{"content":{"parts":[{"text":"partial"}]},"finishReason":"CONTINUATION"}],"continuationToken":"private-token"}).to_string();
        let reply = if streaming {
            ScriptedResponse::sse(&format!("data: {terminal}\n\n"))
        } else {
            ScriptedResponse::json(&terminal)
        };
        let mut server = spawn_scripted_http_server(vec![reply]).await;
        let provider = WireProvider::Gemini.build(server.base_url());
        let model = WireProvider::Gemini.model(&provider, "models/gemini-4-argon");
        let request = WireRequest::user("probe");
        let error = if streaming {
            let mut stream = model.generate_stream(&request).await.unwrap();
            let mut failure = None;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(GenerateEvent::Completed(_)) => panic!("long decode must not complete"),
                    Err(error) => failure = Some(error),
                    _ => {}
                }
            }
            failure.expect("unsupported terminal")
        } else {
            model
                .generate(&request)
                .await
                .expect_err("long decode is unsupported")
        };
        provider.shutdown().await.unwrap();
        server.finish(Duration::from_secs(1)).await.unwrap();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert_eq!(error.context().phase, ErrorPhase::Normalize);
        assert!(!format!("{error:?}").contains("private-token"));
        assert_eq!(server.requests().len(), 1);
    }
}

#[tokio::test]
async fn current_gemini_function_result_media_preserves_exact_call_identity() {
    for model_id in ["models/gemini-4-argon", "models/gemini-3.8-flash"] {
        let reply = json!({"modelVersion":model_id,"candidates":[{"content":{"parts":[{"text":"accepted"}]},"finishReason":"STOP"}]}).to_string();
        let mut server = spawn_scripted_http_server(vec![ScriptedResponse::json(&reply)]).await;
        let provider = WireProvider::Gemini.build(server.base_url());
        let call = FunctionCall::new(Some("call-media".into()), "inspect", json!({})).unwrap();
        let assistant = Message::assistant(Vec::new(), vec![call.clone()]).unwrap();
        let observed = WireProvider::Gemini
            .model(&provider, model_id)
            .generate(&WireRequest::function_output(
                assistant,
                &call,
                vec![ContentPart::Image {
                    media_type: "image/png".into(),
                    data: vec![1],
                }],
            ))
            .await;
        let shutdown = provider.shutdown().await;
        let joined = server.finish(Duration::from_secs(1)).await;
        assert!(
            observed.is_ok(),
            "{model_id}: {observed:?}; cleanup={shutdown:?}/{joined:?}"
        );
        shutdown.unwrap();
        joined.unwrap();
        let requests = server.requests();
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(
            body["contents"][1]["parts"][0]["functionResponse"]["id"],
            "call-media"
        );
        assert_eq!(
            body["contents"][1]["parts"][0]["functionResponse"]["parts"][0]["inlineData"],
            json!({"mimeType":"image/png","data":"AQ=="})
        );
    }
}

#[tokio::test]
async fn gemini_current_batch_terminals_share_generation_normalization() {
    for (native, expected) in [
        ("ESCALATION", Some(FinishReason::ContentFilter)),
        ("PUP_LIMITED_DISABLED", Some(FinishReason::Error)),
        ("CONTINUATION", None),
    ] {
        let reply = json!({"name":"batches/current","metadata":{"state":"JOB_STATE_SUCCEEDED"},"response":{"inlinedResponses":{"inlinedResponses":[{"metadata":{"custom_id":"row"},"response":{"modelVersion":"gemini-4-argon","candidates":[{"finishReason":native}]}}]}}}).to_string();
        let mut server = spawn_scripted_http_server(vec![ScriptedResponse::json(&reply)]).await;
        let provider = WireProvider::Gemini.build(server.base_url());
        let observed = provider
            .batch_results("batches/current".into(), NonZeroUsize::new(8192).unwrap())
            .await;
        provider.shutdown().await.unwrap();
        server.finish(Duration::from_secs(1)).await.unwrap();
        match expected {
            Some(expected) => {
                let rows = observed.unwrap();
                let [BatchResult::Succeeded { custom_id, output }] = rows.as_slice() else {
                    panic!("expected one normalized terminal")
                };
                assert_eq!(custom_id, "row");
                assert_eq!(output.finish_reason, Some(expected));
            }
            None => {
                let error = observed.expect_err("long decode cannot be a successful batch row");
                assert_eq!(error.kind(), ErrorKind::Unsupported);
                assert_eq!(error.context().phase, ErrorPhase::Normalize);
            }
        }
        assert_eq!(server.requests().len(), 1);
    }
}
