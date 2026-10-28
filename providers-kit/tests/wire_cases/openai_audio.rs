use std::time::Duration;

use crate::support::scripted_http::{ScriptedHttpServer, spawn_scripted_http_server};
use providers_kit::batch::BatchSubmitRequest;
use providers_kit::config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig};
use providers_kit::error::ErrorKind;
use providers_kit::generation::{ContentPart, GenerateRequest, Message};
use providers_kit::{ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig};

struct AudioRequest;

impl AudioRequest {
    fn provider(server: &ScriptedHttpServer) -> Provider {
        let mut url = server.base_url();
        url.set_path("/v1");
        Provider::from_config(ProviderConfig::new(
            ProviderKind::OpenAi(
                OpenAiConfig::new(ApiKey::new("audio-wire-key").unwrap()).with_base_url(url),
            ),
            TransportConfig::default(),
        ))
        .unwrap()
    }

    fn request() -> GenerateRequest {
        GenerateRequest::new(vec![
            Message::user(vec![ContentPart::Audio {
                media_type: "audio/wav".into(),
                data: b"RIFF-test-audio".to_vec(),
            }])
            .unwrap(),
        ])
        .unwrap()
    }
}

#[tokio::test]
async fn openai_stateless_audio_models_are_unsupported_before_io() {
    let mut server = spawn_scripted_http_server(vec![]).await;
    let provider = AudioRequest::provider(&server);
    let text_history = GenerateRequest::new(vec![
        Message::user_text("Earlier input").unwrap(),
        Message::assistant_text("Earlier answer").unwrap(),
        Message::user_text("Continue using text only").unwrap(),
    ])
    .unwrap();
    let mut outcomes = Vec::new();
    for id in [
        "gpt-audio-1.5",
        "gpt-audio-mini",
        "gpt-audio-mini-2025-12-15",
        "gpt-4o-audio-preview",
        "gpt-4o-audio-preview-2025-06-03",
    ] {
        let model = provider.model(ModelId::new(id).unwrap()).unwrap();
        for request in [&AudioRequest::request(), &text_history] {
            outcomes.push((
                id,
                "unary",
                model
                    .generate(request)
                    .await
                    .err()
                    .map(|error| error.kind()),
            ));
            outcomes.push((
                id,
                "stream",
                model
                    .generate_stream(request)
                    .await
                    .err()
                    .map(|error| error.kind()),
            ));
        }
    }
    let shutdown = provider.shutdown().await;
    let settled = server.finish(Duration::from_millis(100)).await;
    assert!(shutdown.is_ok(), "provider shutdown failed: {shutdown:?}");
    assert_eq!(settled, Ok(()));
    assert!(
        outcomes
            .iter()
            .all(|(_, _, kind)| *kind == Some(ErrorKind::Unsupported)),
        "stateless audio must reject every exact selection and text-only history: {outcomes:?}"
    );
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn openai_stateless_audio_input_is_unsupported_on_responses_before_io() {
    let mut server = spawn_scripted_http_server(vec![]).await;
    let provider = AudioRequest::provider(&server);
    let model = provider.model(ModelId::new("gpt-6-luna").unwrap()).unwrap();
    let request = AudioRequest::request();
    let unary = model.generate(&request).await;
    let streamed = model.generate_stream(&request).await;
    let shutdown = provider.shutdown().await;
    let settled = server.finish(Duration::from_millis(100)).await;
    assert!(shutdown.is_ok(), "provider shutdown failed: {shutdown:?}");
    assert_eq!(settled, Ok(()));
    assert!(matches!(unary, Err(error) if error.kind() == ErrorKind::Unsupported));
    assert!(matches!(streamed, Err(error) if error.kind() == ErrorKind::Unsupported));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn openai_stateless_audio_batches_are_unsupported_before_io() {
    use providers_kit::batch::BatchInput;
    let mut server = spawn_scripted_http_server(vec![]).await;
    let provider = AudioRequest::provider(&server);
    let inline = BatchSubmitRequest::Inline {
        rows: vec![BatchInput::new("audio", AudioRequest::request()).unwrap()],
    };
    let text = BatchSubmitRequest::Inline {
        rows: vec![
            BatchInput::new(
                "text",
                GenerateRequest::new(vec![Message::user_text("Text only").unwrap()]).unwrap(),
            )
            .unwrap(),
        ],
    };
    let borrowed = BatchSubmitRequest::ProviderFile {
        file_id: "file-borrowed-audio".into(),
    };
    let mut outcomes = Vec::new();
    for id in [
        "gpt-audio-1.5",
        "gpt-audio-mini",
        "gpt-audio-mini-2025-12-15",
        "gpt-4o-audio-preview",
        "gpt-4o-audio-preview-2025-06-03",
    ] {
        let model = provider.model(ModelId::new(id).unwrap()).unwrap();
        for request in [&inline, &text, &borrowed] {
            outcomes.push((
                id,
                model
                    .submit_batch(request)
                    .await
                    .err()
                    .map(|error| error.kind()),
            ));
        }
    }
    let ordinary = provider.model(ModelId::new("gpt-6-luna").unwrap()).unwrap();
    outcomes.push((
        "gpt-6-luna",
        ordinary
            .submit_batch(&inline)
            .await
            .err()
            .map(|error| error.kind()),
    ));
    let shutdown = provider.shutdown().await;
    let settled = server.finish(Duration::from_millis(100)).await;
    assert!(shutdown.is_ok(), "provider shutdown failed: {shutdown:?}");
    assert_eq!(settled, Ok(()));
    assert!(
        outcomes
            .iter()
            .all(|(_, kind)| *kind == Some(ErrorKind::Unsupported)),
        "batch must reject before upload or borrowed input read: {outcomes:?}"
    );
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn openai_stateless_audio_preserves_closed_precedence() {
    use providers_kit::batch::BatchInput;
    let mut server = spawn_scripted_http_server(vec![]).await;
    let provider = AudioRequest::provider(&server);
    let audio = provider
        .model(ModelId::new("gpt-audio-1.5").unwrap())
        .unwrap();
    let ordinary = provider.model(ModelId::new("gpt-6-luna").unwrap()).unwrap();
    let shutdown = provider.shutdown().await;
    let request = AudioRequest::request();
    let mut outcomes = Vec::new();
    for model in [&audio, &ordinary] {
        outcomes.push(
            model
                .generate(&request)
                .await
                .err()
                .map(|error| error.kind()),
        );
        outcomes.push(
            model
                .generate_stream(&request)
                .await
                .err()
                .map(|error| error.kind()),
        );
        outcomes.push(
            model
                .submit_batch(&BatchSubmitRequest::Inline {
                    rows: vec![BatchInput::new("audio", request.clone()).unwrap()],
                })
                .await
                .err()
                .map(|error| error.kind()),
        );
    }
    outcomes.push(
        audio
            .submit_batch(&BatchSubmitRequest::ProviderFile {
                file_id: "file-borrowed-audio".into(),
            })
            .await
            .err()
            .map(|error| error.kind()),
    );
    let settled = server.finish(Duration::from_millis(100)).await;
    assert!(shutdown.is_ok(), "provider shutdown failed: {shutdown:?}");
    assert_eq!(settled, Ok(()));
    assert!(
        outcomes.iter().all(|kind| *kind == Some(ErrorKind::Closed)),
        "shutdown must win over audio rejection: {outcomes:?}"
    );
    assert!(server.requests().is_empty());
}
