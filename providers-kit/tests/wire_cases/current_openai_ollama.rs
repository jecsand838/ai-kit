use std::num::NonZeroU32;
use std::time::Duration;

use futures_util::StreamExt;
use providers_kit::embedding::EmbedRequest;
use providers_kit::error::{ErrorKind, ErrorPhase, ResponseViolation};
use providers_kit::generation::{
    GenerateEvent, GenerationConfig, ReasoningConfig, ReasoningEffort, ReasoningSummaryMode,
};
use providers_kit::{ModelBackend, ProviderBackend};
use serde_json::{Value, json};

use crate::support::scripted_http::{ScriptedResponse, spawn_scripted_http_server};
use crate::{WireProvider, WireRequest};

#[tokio::test]
async fn current_ollama_gemma_boolean_thinking_is_exact_and_not_summary_disabling() {
    let mut server = spawn_scripted_http_server(vec![ScriptedResponse::json(r#"{"model":"gemma4:e4b-it-qat","done":true,"done_reason":"stop","message":{"role":"assistant","content":"answer"}}"#)]).await;
    let mut base = server.base_url();
    base.set_path("/api");
    let provider = WireProvider::Ollama.build(base);
    let model = WireProvider::Ollama.model(&provider, "gemma4:e4b-it-qat");
    let request = WireRequest::user("answer").with_reasoning(ReasoningConfig::default());
    let observed = model.generate(&request).await;
    let shutdown = provider.shutdown().await;
    let settled = server.finish(Duration::from_secs(1)).await;
    assert!(
        observed.is_ok(),
        "{observed:?}; cleanup={shutdown:?}/{settled:?}"
    );
    shutdown.unwrap();
    settled.unwrap();
    let requests = server.requests();
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["think"], true);
}

#[tokio::test]
async fn current_ollama_direct_cloud_format_rejection_uses_effective_origin() {
    use providers_kit::config::{OllamaConfig, ProviderKind, RetryPolicy, TransportConfig};
    use providers_kit::generation::{JsonSchema, ResponseFormat};
    use providers_kit::{Provider, ProviderConfig};
    for format in [
        ResponseFormat::JsonObject,
        ResponseFormat::JsonSchema(
            JsonSchema::new(json!({"type":"object","properties":{}})).unwrap(),
        ),
    ] {
        let config = OllamaConfig::local()
            .with_base_url("https://ollama.com/api".parse().unwrap())
            .unwrap();
        let transport = TransportConfig::new(
            Duration::from_millis(10),
            Duration::from_millis(10),
            Duration::from_secs(1),
            RetryPolicy::default(),
        )
        .unwrap();
        let provider =
            Provider::from_config(ProviderConfig::new(ProviderKind::Ollama(config), transport))
                .unwrap();
        let model = WireProvider::Ollama.model(&provider, "gpt-oss:20b");
        let request = WireRequest::user("JSON").with_response_format(format);
        let observed = model.generate(&request).await;
        let shutdown = provider.shutdown().await;
        assert!(
            matches!(&observed, Err(error) if error.kind() == ErrorKind::Unsupported && error.context().phase == ErrorPhase::InputValidation),
            "{observed:?}; cleanup={shutdown:?}"
        );
        shutdown.unwrap();
    }
}

#[tokio::test]
async fn current_openai_exact_profiles_render_portable_effort_and_output_limit() {
    for (model_id, minimum) in [
        ("gpt-6-astra", "low"),
        ("gpt-6.1-sol", "low"),
        ("gpt-6-sol", "none"),
        ("gpt-6-luna", "none"),
    ] {
        for (effort, native) in [
            (ReasoningEffort::Minimal, minimum),
            (ReasoningEffort::Low, "low"),
            (ReasoningEffort::Medium, "medium"),
            (ReasoningEffort::High, "high"),
            (ReasoningEffort::Maximum, "max"),
        ] {
            let mut server = spawn_scripted_http_server(vec![ScriptedResponse::json(&json!({"model": model_id, "status":"completed", "output":[{"type":"message", "role":"assistant", "content":[{"type":"output_text", "text":"answer"}]}]}).to_string())]).await;
            let provider = WireProvider::OpenAi.build(server.base_url());
            let model = WireProvider::OpenAi.model(&provider, model_id);
            let request = WireRequest::user("answer")
                .with_generation(
                    GenerationConfig::default()
                        .with_max_output_tokens(NonZeroU32::new(32).unwrap()),
                )
                .unwrap()
                .with_reasoning(ReasoningConfig {
                    effort: Some(effort),
                    max_reasoning_tokens: None,
                    summary_mode: ReasoningSummaryMode::Hidden,
                });
            let result = model.generate(&request).await;
            let shutdown = provider.shutdown().await;
            let settled = server.finish(Duration::from_secs(1)).await;
            assert!(
                result.is_ok(),
                "{model_id} {effort:?}: {result:?}; cleanup={shutdown:?}/{settled:?}"
            );
            shutdown.unwrap();
            settled.unwrap();
            let requests = server.requests();
            assert_eq!(requests.len(), 1);
            let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
            assert_eq!(body["model"], model_id);
            assert_eq!(body["reasoning"]["effort"], native);
            assert_eq!(body["max_output_tokens"], 32);
            assert!(body["reasoning"].get("summary").is_none());
        }
    }
}

#[tokio::test]
async fn current_openai_sampling_requires_explicit_none_on_optional_reasoning_models() {
    for model_id in ["gpt-6-luna", "gpt-6-sol"] {
        let mut server = spawn_scripted_http_server(vec![ScriptedResponse::json(&json!({"model": model_id,"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}).to_string())]).await;
        let provider = WireProvider::OpenAi.build(server.base_url());
        let model = WireProvider::OpenAi.model(&provider, model_id);
        let request = WireRequest::user("answer")
            .with_generation(GenerationConfig::default().with_temperature(0.25).unwrap())
            .unwrap();
        let omitted = model.generate(&request).await.unwrap_err();
        assert_eq!(omitted.kind(), ErrorKind::Unsupported);
        assert!(server.requests().is_empty());
        let request = request.with_reasoning(ReasoningConfig {
            effort: Some(ReasoningEffort::Minimal),
            max_reasoning_tokens: None,
            summary_mode: ReasoningSummaryMode::Hidden,
        });
        let result = model.generate(&request).await;
        let shutdown = provider.shutdown().await;
        let settled = server.finish(Duration::from_secs(1)).await;
        assert!(
            result.is_ok(),
            "{result:?}; cleanup={shutdown:?}/{settled:?}"
        );
        shutdown.unwrap();
        settled.unwrap();
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["temperature"], 0.25);
        assert_eq!(body["reasoning"]["effort"], "none");
    }
}

#[tokio::test]
async fn current_ollama_embedding_dimensions_preserve_vectors_and_validate_every_row() {
    for vectors in [json!([[0.1, 0.2], [0.3, 0.4]]), json!([[0.1, 0.2], [0.3]])] {
        let mut server = spawn_scripted_http_server(vec![ScriptedResponse::json(
            &json!({"model":"embeddinggemma","embeddings":vectors}).to_string(),
        )])
        .await;
        let mut base = server.base_url();
        base.set_path("/api");
        let provider = WireProvider::Ollama.build(base);
        let model = WireProvider::Ollama.model(&provider, "embeddinggemma");
        let result = model
            .embed(
                &EmbedRequest::new(vec!["first".into(), "second".into()])
                    .unwrap()
                    .with_dimensions(NonZeroU32::new(2).unwrap()),
            )
            .await;
        let shutdown = provider.shutdown().await;
        let settled = server.finish(Duration::from_secs(1)).await;
        let requests = server.requests();
        assert_eq!(
            requests.len(),
            1,
            "result={result:?}; cleanup={shutdown:?}/{settled:?}"
        );
        shutdown.unwrap();
        settled.unwrap();
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["dimensions"], 2);
        assert_eq!(body["input"], json!(["first", "second"]));
        if vectors[1].as_array().unwrap().len() == 2 {
            assert_eq!(
                result.unwrap().vectors,
                vec![vec![0.1, 0.2], vec![0.3, 0.4]]
            );
        } else {
            let error = result.unwrap_err();
            assert_eq!(
                error.kind(),
                ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation {
                    rule: "embedding_dimensions"
                })
            );
            assert_eq!(error.context().phase, ErrorPhase::Normalize);
        }
    }
}

#[tokio::test]
async fn current_ollama_penalties_are_forwarded_without_clamping() {
    let mut server = spawn_scripted_http_server(vec![ScriptedResponse::json(r#"{"model":"gemma4:e4b-it-qat","done":true,"done_reason":"stop","message":{"role":"assistant","content":"answer"}}"#)]).await;
    let mut base = server.base_url();
    base.set_path("/api");
    let provider = WireProvider::Ollama.build(base);
    let model = WireProvider::Ollama.model(&provider, "gemma4:e4b-it-qat");
    let request = WireRequest::user("answer")
        .with_generation(
            GenerationConfig::default()
                .with_presence_penalty(-0.5)
                .unwrap()
                .with_frequency_penalty(0.75)
                .unwrap(),
        )
        .unwrap();
    let result = model.generate(&request).await;
    let shutdown = provider.shutdown().await;
    let settled = server.finish(Duration::from_secs(1)).await;
    assert!(
        result.is_ok(),
        "{result:?}; cleanup={shutdown:?}/{settled:?}"
    );
    shutdown.unwrap();
    settled.unwrap();
    let requests = server.requests();
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["options"]["presence_penalty"], -0.5);
    assert_eq!(body["options"]["frequency_penalty"], 0.75);
}

#[tokio::test]
async fn current_ollama_cached_usage_preserves_absence_zero_and_cache_only_stream_updates() {
    for cached in [None, Some(0), Some(7)] {
        for streaming in [false, true] {
            let mut response = json!({"model":"gemma4:e4b-it-qat","done":true,"done_reason":"stop","message":{"role":"assistant","content":"answer"}});
            if let Some(count) = cached {
                response["prompt_eval_cached_count"] = json!(count);
            }
            let reply = ScriptedResponse::json(&format!("{response}\n"));
            let mut server = spawn_scripted_http_server(vec![reply]).await;
            let mut base = server.base_url();
            base.set_path("/api");
            let provider = WireProvider::Ollama.build(base);
            let model = WireProvider::Ollama.model(&provider, "gemma4:e4b-it-qat");
            let result = if streaming {
                let mut stream = model
                    .generate_stream(&WireRequest::user("answer"))
                    .await
                    .unwrap();
                let mut final_result = None;
                while let Some(event) = stream.next().await {
                    match event.unwrap() {
                        GenerateEvent::Usage(usage) => {
                            assert_eq!(usage.cached_input_tokens, cached)
                        }
                        GenerateEvent::Completed(response) => final_result = Some(*response),
                        _ => {}
                    }
                }
                final_result.unwrap()
            } else {
                model.generate(&WireRequest::user("answer")).await.unwrap()
            };
            let shutdown = provider.shutdown().await;
            let settled = server.finish(Duration::from_secs(1)).await;
            assert_eq!(
                result.usage.cached_input_tokens, cached,
                "streaming={streaming}; cleanup={shutdown:?}/{settled:?}"
            );
            assert_eq!(result.usage.input_tokens, None);
            shutdown.unwrap();
            settled.unwrap();
        }
    }
}
