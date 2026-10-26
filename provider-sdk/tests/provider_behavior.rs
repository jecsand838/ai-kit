use provider_sdk::config::{
    AnthropicConfig, ApiKey, GeminiConfig, OllamaConfig, OpenAiConfig, ProviderKind,
    TransportConfig,
};
use provider_sdk::embedding::EmbedRequest;
use provider_sdk::error::ErrorKind;
use provider_sdk::files::FileUploadRequest;
use provider_sdk::generation::{
    ContentPart, FunctionCall, GenerateRequest, GenerationConfig, Message, ReasoningConfig,
    ReasoningEffort, Role,
};
use provider_sdk::models::ModelId;
use provider_sdk::session::SessionConfig;
use provider_sdk::{ModelBackend, Provider, ProviderBackend, ProviderConfig};
use serde_json::json;
use std::num::NonZeroU32;
use url::Url;

fn key(value: &str) -> ApiKey {
    ApiKey::new(value).expect("fixture key should validate")
}

fn provider(kind: ProviderKind) -> Provider {
    Provider::from_config(ProviderConfig::new(kind, TransportConfig::default()))
        .expect("fixture provider should construct without network I/O")
}

#[test]
fn file_upload_requests_reject_header_unsafe_values_during_construction_and_deserialization() {
    for (file_name, media_type) in [
        (" padded.txt ", "text/plain"),
        ("unsafe\nname.txt", "text/plain"),
        ("data.txt", " text/plain"),
        ("data.txt", "text/plain/extra"),
        ("data.txt", "text/plain;"),
    ] {
        assert!(
            matches!(
                FileUploadRequest::new(file_name, media_type, vec![1]),
                Err(ref error) if error.kind() == ErrorKind::InvalidInput
            ),
            "unsafe upload values were accepted: {file_name:?}, {media_type:?}"
        );
        assert!(
            serde_json::from_value::<FileUploadRequest>(json!({
                "file_name": file_name,
                "media_type": media_type,
                "data": "AQ=="
            }))
            .is_err()
        );
    }
    FileUploadRequest::new("data.txt", "text/plain; charset=utf-8", vec![1])
        .expect("valid media type parameters should remain accepted");
}

#[tokio::test]
async fn sessionless_models_report_unsupported_without_io() {
    let ollama = provider(ProviderKind::Ollama(
        OllamaConfig::local()
            .with_base_url(Url::parse("http://127.0.0.1:9/api").expect("URL should parse"))
            .expect("exact /api base should validate"),
    ));
    let ollama_model = ollama
        .model(ModelId::new("chat-model").expect("model id should validate"))
        .expect("exact model should construct");
    assert!(matches!(
        ollama_model.open_session(SessionConfig::default()).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));

    let anthropic = provider(ProviderKind::Anthropic(
        AnthropicConfig::new(key("anthropic-test"))
            .with_base_url(Url::parse("http://127.0.0.1:9").expect("URL should parse")),
    ));
    let anthropic_model = anthropic
        .model(ModelId::new("claude-test").expect("model id should validate"))
        .expect("exact model should construct");
    let embedding =
        EmbedRequest::new(vec!["hello".to_string()]).expect("embedding should validate");
    assert!(matches!(
        anthropic_model.embed(&embedding).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    assert!(matches!(
        anthropic_model.open_session(SessionConfig::default()).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));

    ollama.shutdown().await.expect("shutdown should succeed");
    anthropic.shutdown().await.expect("shutdown should succeed");
}

#[test]
fn gemini_requires_the_exact_resource_model_id() {
    let gemini = provider(ProviderKind::Gemini(
        GeminiConfig::new(key("gemini-test"))
            .with_base_url(Url::parse("http://127.0.0.1:9").expect("URL should parse")),
    ));
    let error = gemini
        .model(ModelId::new("gemini-2.5-flash").expect("model id should validate"))
        .expect_err("Gemini model ids must retain the models/ resource prefix");
    assert!(matches!(error.kind(), ErrorKind::InvalidInput));

    let model = gemini
        .model(ModelId::new("models/gemini-2.5-flash").expect("model id should validate"))
        .expect("exact Gemini resource id should construct");
    assert_eq!(model.model_id().as_str(), "models/gemini-2.5-flash");
}

#[tokio::test]
async fn model_metadata_rejects_path_injection_before_io() {
    let openai = provider(ProviderKind::OpenAi(
        OpenAiConfig::new(key("openai-test"))
            .with_base_url(Url::parse("http://127.0.0.1:9/v1").expect("URL should parse")),
    ));
    assert!(matches!(
        openai
            .model_info(ModelId::new("gpt-safe?leak=1").expect("model id should validate"))
            .await,
        Err(ref error) if error.kind() == ErrorKind::InvalidInput
    ));

    let anthropic = provider(ProviderKind::Anthropic(
        AnthropicConfig::new(key("anthropic-test"))
            .with_base_url(Url::parse("http://127.0.0.1:9").expect("URL should parse")),
    ));
    assert!(matches!(
        anthropic
            .model_info(ModelId::new("claude-safe/extra").expect("model id should validate"))
            .await,
        Err(ref error) if error.kind() == ErrorKind::InvalidInput
    ));

    let gemini = provider(ProviderKind::Gemini(
        GeminiConfig::new(key("gemini-test"))
            .with_base_url(Url::parse("http://127.0.0.1:9").expect("URL should parse")),
    ));
    assert!(matches!(
        gemini
            .model(ModelId::new("models/gemini-safe#fragment").expect("model id should validate")),
        Err(ref error) if error.kind() == ErrorKind::InvalidInput
    ));
}

#[tokio::test]
async fn interactive_model_and_audio_compatibility_is_checked_before_websocket_io() {
    let openai = provider(ProviderKind::OpenAi(
        OpenAiConfig::new(key("openai-test"))
            .with_base_url(Url::parse("http://127.0.0.1:9/v1").expect("URL should parse")),
    ));
    let openai_stateless = openai
        .model(ModelId::new("gpt-standard").expect("model id should validate"))
        .expect("exact stateless model should construct");
    assert!(matches!(
        openai_stateless
            .open_session(SessionConfig::default())
            .await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    let openai_model = openai
        .model(ModelId::new("gpt-realtime").expect("model id should validate"))
        .expect("exact model should construct");
    let unsupported_openai_audio = SessionConfig::default()
        .with_input_audio("audio/opus", false)
        .expect("portable audio media type should validate");
    assert!(matches!(
        openai_model.open_session(unsupported_openai_audio).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));

    let gemini = provider(ProviderKind::Gemini(
        GeminiConfig::new(key("gemini-test"))
            .with_base_url(Url::parse("http://127.0.0.1:9").expect("URL should parse")),
    ));
    let gemini_stateless = gemini
        .model(ModelId::new("models/gemini-2.5-flash").expect("model id should validate"))
        .expect("exact stateless model should construct");
    assert!(matches!(
        gemini_stateless
            .open_session(SessionConfig::default())
            .await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    let gemini_live = gemini
        .model(
            ModelId::new("models/gemini-3.1-flash-live-preview").expect("model id should validate"),
        )
        .expect("exact live model should construct");
    assert!(matches!(
        gemini_live.open_session(SessionConfig::default()).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));
    let unsupported_gemini_audio = SessionConfig::default()
        .with_output_audio("audio/opus", false)
        .expect("portable audio media type should validate");
    assert!(matches!(
        gemini_live.open_session(unsupported_gemini_audio).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));

    openai.shutdown().await.expect("shutdown should succeed");
    gemini.shutdown().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn anthropic_reasoning_uses_only_exact_known_model_profiles() {
    let anthropic = provider(ProviderKind::Anthropic(
        AnthropicConfig::new(key("anthropic-test"))
            .with_base_url(Url::parse("http://127.0.0.1:9").expect("URL should parse")),
    ));
    let request = |reasoning| {
        GenerateRequest::new(vec![
            Message::user_text("reason").expect("message should validate"),
        ])
        .expect("request should validate")
        .with_generation(
            GenerationConfig::default()
                .with_max_output_tokens(NonZeroU32::new(2048).expect("limit is non-zero")),
        )
        .expect("generation controls should validate")
        .with_reasoning(reasoning)
    };
    let effort = request(ReasoningConfig {
        effort: Some(ReasoningEffort::Maximum),
        ..ReasoningConfig::default()
    });
    let budget = request(ReasoningConfig {
        max_reasoning_tokens: Some(1024),
        ..ReasoningConfig::default()
    });

    for id in [
        "claude-fable-5",
        "claude-opus-5",
        "claude-sonnet-5",
        "claude-opus-4-8",
        "claude-opus-4-7",
    ] {
        let model = anthropic
            .model(ModelId::new(id).expect("model id should validate"))
            .expect("exact model should construct");
        assert!(matches!(
            model.generate(&effort).await,
            Err(ref error) if error.kind() == ErrorKind::Transport
        ));
        assert!(matches!(
            model.generate(&budget).await,
            Err(ref error) if error.kind() == ErrorKind::Unsupported
        ));
    }

    for id in ["claude-opus-4-6", "claude-sonnet-4-6"] {
        let model = anthropic
            .model(ModelId::new(id).expect("model id should validate"))
            .expect("exact model should construct");
        assert!(matches!(
            model.generate(&effort).await,
            Err(ref error) if error.kind() == ErrorKind::Transport
        ));
        assert!(matches!(
            model.generate(&budget).await,
            Err(ref error) if error.kind() == ErrorKind::Transport
        ));
    }

    let haiku = anthropic
        .model(ModelId::new("claude-haiku-4-5-20251001").expect("model id should validate"))
        .expect("exact model should construct");
    assert!(matches!(
        haiku.generate(&budget).await,
        Err(ref error) if error.kind() == ErrorKind::Transport
    ));
    assert!(matches!(
        haiku.generate(&effort).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));

    let unknown = anthropic
        .model(ModelId::new("claude-unknown").expect("model id should validate"))
        .expect("exact model should construct");
    assert!(matches!(
        unknown.generate(&effort).await,
        Err(ref error) if error.kind() == ErrorKind::Unsupported
    ));

    anthropic.shutdown().await.expect("shutdown should succeed");
}

#[test]
fn function_calls_are_constructed_on_assistant_messages() {
    let call = FunctionCall::new(None, "lookup", json!({})).expect("function call should validate");
    let message: Message = Message::assistant(
        vec![ContentPart::Text {
            text: "hello".to_string(),
        }],
        vec![call],
    )
    .expect("assistant message with function call should validate");
    assert_eq!(message.role(), Role::Assistant);
    assert_eq!(message.function_calls().len(), 1);
}
