use provider_sdk::error::{ErrorKind, ErrorPhase, NativeErrorDetails, Operation};
use std::convert::Infallible;
use std::num::NonZeroU32;

use provider_sdk::batch::{BatchInput, BatchResult, BatchSubmitRequest};
use provider_sdk::config::{ApiKey, OllamaConfig, OpenAiConfig, ProviderKind, TransportConfig};
use provider_sdk::generation::{
    Artifact, Citation, ContentPart, FileReference, FinishReason, FunctionCall, FunctionResult,
    GenerateEvent, GenerateRequest, GenerateResponse, GenerationConfig, GenerationContinuation,
    JsonSchema, Message, ProviderFunctionTool, ProviderToolSpec, ReasoningConfig, ReasoningEffort,
    ReasoningSummaryMode, ResponseFormat, Role, ToolChoice, Usage,
};
use provider_sdk::hosted::{HostedTool, HostedToolOutput, HostedToolStatus};
use provider_sdk::models::{
    Model, ModelBackend, ModelId, ModelListRequest, ModelPage, ModelSummary,
};
use provider_sdk::session::{Session, SessionBackend, SessionConfig, SessionTurnDetection};
use provider_sdk::{Provider, ProviderBackend, ProviderConfig, SdkError};
use serde_json::json;
use url::Url;

fn assert_backend_hierarchy()
where
    Provider: ProviderBackend<Model = Model>,
    Model: ModelBackend<Continuation = GenerationContinuation, Session = Session>,
    Session: SessionBackend,
    Infallible: SessionBackend,
{
}

#[test]
fn root_and_functional_modules_support_the_primary_workflow() {
    assert_backend_hierarchy();
    let provider = Provider::from_config(ProviderConfig::new(
        ProviderKind::Ollama(OllamaConfig::local()),
        TransportConfig::default(),
    ))
    .expect("Ollama construction should be local and deterministic");
    let id = ModelId::new("exact/model:id").expect("exact model id should validate");
    let model = provider
        .model(id.clone())
        .expect("exact Ollama model lookup should construct a child");
    assert_eq!(model.model_id(), &id);

    let generation = GenerationConfig::default()
        .with_max_output_tokens(NonZeroU32::new(64).expect("nonzero tokens"))
        .with_tool_choice(ToolChoice::Auto);
    let request: GenerateRequest = GenerateRequest::new(vec![
        Message::system(vec![ContentPart::Text {
            text: "Be concise.".to_string(),
        }])
        .expect("instruction should validate"),
        Message::user_text("hello").expect("user message should validate"),
    ])
    .expect("leading instructions and one user message should validate")
    .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::WebSearch)])
    .expect("hosted tool should validate")
    .with_generation(generation)
    .expect("generation controls should validate");
    assert_eq!(request.messages().len(), 2);
    assert_eq!(request.tools().len(), 1);
    assert_eq!(request.generation().tool_choice(), Some(&ToolChoice::Auto));

    let session = SessionConfig::default().with_turn_detection(SessionTurnDetection::Automatic);
    assert_eq!(session.turn_detection(), SessionTurnDetection::Automatic);
}

#[test]
fn validated_function_messages_round_trip_without_serializing_continuation() {
    assert!(Message::<String>::assistant(Vec::new(), Vec::new()).is_err());
    assert!(
        serde_json::from_value::<Message<String>>(json!({
            "role": "assistant", "content": [], "function_calls": [], "function_results": []
        }))
        .is_err()
    );
    let call = FunctionCall::new(
        Some("call_1".to_string()),
        "lookup_weather",
        json!({ "city": "Chicago" }),
    )
    .expect("object arguments should validate");
    let assistant = Message::<String>::assistant(Vec::new(), vec![call.clone()])
        .expect("assistant function call should validate");
    assert_eq!(assistant.role(), Role::Assistant);
    assert_eq!(assistant.function_calls(), std::slice::from_ref(&call));
    let encoded = serde_json::to_value(&assistant).expect("plain assistant should serialize");
    let decoded: Message<String> =
        serde_json::from_value(encoded).expect("assistant should deserialize");
    assert_eq!(decoded, assistant);

    let continued = assistant
        .with_continuation("opaque-state".to_string())
        .expect("assistant continuation should attach");
    assert!(serde_json::to_value(&continued).is_err());

    let result = FunctionResult::new(
        call.id().map(str::to_owned),
        call.name().to_owned(),
        vec![ContentPart::Text {
            text: "72 F".to_string(),
        }],
        false,
    )
    .expect("function output should validate");
    let tool = Message::<String>::tool(vec![result.clone()]).expect("tool message should validate");
    assert_eq!(tool.role(), Role::Tool);
    assert_eq!(tool.function_results(), &[result]);
    let encoded = serde_json::to_value(&tool).expect("tool message should serialize");
    let decoded: Message<String> =
        serde_json::from_value(encoded).expect("tool message should deserialize");
    assert_eq!(decoded.function_results().len(), 1);
    assert!(decoded.continuation().is_none());
}

#[test]
fn validated_opaque_request_owners_reject_invalid_values() {
    assert!(GenerateRequest::<()>::new(Vec::new()).is_err());
    assert!(ModelId::new("").is_err());
    assert!(ModelId::new(" padded ").is_err());
    assert!(FunctionCall::new(None, "call", json!("not an object")).is_err());
    assert!(FunctionCall::new(None, "bad name", json!({})).is_err());
    assert!(FunctionCall::new(None, "9starts_with_a_digit", json!({})).is_err());
    assert!(FunctionResult::new(None, "call", Vec::new(), false).is_err());
    assert!(JsonSchema::new(json!({"type": "array"})).is_err());
    assert!(
        Message::<()>::system(vec![ContentPart::Text {
            text: String::new(),
        }])
        .is_err()
    );
    assert!(
        Message::<()>::user(vec![ContentPart::Image {
            media_type: "text/plain".to_string(),
            data: vec![1],
        }])
        .is_err()
    );
    assert!(
        Message::<()>::user(vec![ContentPart::ImageUrl {
            url: Url::parse("file:///tmp/image.png").expect("fixture URL should parse"),
        }])
        .is_err()
    );
    assert!(
        FunctionResult::new(
            None,
            "call",
            vec![ContentPart::File {
                reference: FileReference::Provider {
                    id: String::new(),
                    media_type: None,
                },
            }],
            false,
        )
        .is_err()
    );
    assert!(ApiKey::new("invalid\nkey").is_err());
    assert!(
        OpenAiConfig::new(ApiKey::new("valid-key").expect("fixture key should validate"))
            .with_organization("invalid\norganization")
            .is_err()
    );

    for choice in [ToolChoice::Required, ToolChoice::Any] {
        let request = GenerateRequest::<()>::new(vec![
            Message::user_text("use a tool").expect("user text should validate"),
        ])
        .expect("base request should validate");
        assert!(
            request
                .with_generation(GenerationConfig::default().with_tool_choice(choice))
                .is_err(),
            "forced tool choice without a function declaration must fail"
        );
    }
    let hosted_only = GenerateRequest::<()>::new(vec![
        Message::user_text("search").expect("user text should validate"),
    ])
    .expect("base request should validate")
    .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::WebSearch)])
    .expect("automatic hosted-tool selection should validate");
    assert!(
        hosted_only
            .with_generation(GenerationConfig::default().with_tool_choice(ToolChoice::Required),)
            .is_ok()
    );

    let schema = JsonSchema::new(json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    }))
    .expect("object schema should validate");
    let declaration = ProviderFunctionTool::new("lookup", None, schema)
        .expect("function declaration should validate");
    assert_eq!(declaration.name(), "lookup");
    assert_eq!(declaration.input_schema().schema()["type"], "object");
    let request = GenerateRequest::<()>::new(vec![
        Message::user_text("use lookup").expect("user text should validate"),
    ])
    .expect("base request should validate")
    .with_tools(vec![ProviderToolSpec::Function(declaration)])
    .expect("function declaration should attach");
    assert!(
        request
            .with_generation(
                GenerationConfig::default().with_tool_choice(ToolChoice::Tool {
                    name: "bad name".to_string(),
                }),
            )
            .is_err()
    );
}

#[test]
fn continuation_mappers_preserve_every_non_continuation_field() {
    let model = ModelId::new("model-a").expect("model id should validate");
    let continued = Message::assistant_text("continued")
        .expect("assistant message should validate")
        .with_continuation("opaque-state".to_string())
        .expect("continuation should attach");
    let schema = JsonSchema::new(json!({
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
        "additionalProperties": false
    }))
    .expect("object schema should validate");
    let tool =
        ProviderFunctionTool::new("lookup", Some("Lookup fixture".to_string()), schema.clone())
            .expect("function tool should validate");
    let generation = GenerationConfig::default()
        .with_temperature(0.25)
        .expect("temperature should validate")
        .with_top_p(0.75)
        .expect("top-p should validate")
        .with_top_k(NonZeroU32::new(7).expect("top-k should be nonzero"))
        .with_max_output_tokens(NonZeroU32::new(64).expect("token limit should be nonzero"))
        .with_stop_sequences(vec!["done".to_string()])
        .expect("stop sequence should validate")
        .with_presence_penalty(0.5)
        .expect("presence penalty should validate")
        .with_frequency_penalty(-0.5)
        .expect("frequency penalty should validate")
        .with_seed(17)
        .with_tool_choice(ToolChoice::Tool {
            name: "lookup".to_string(),
        });
    let reasoning = ReasoningConfig {
        effort: Some(ReasoningEffort::High),
        max_reasoning_tokens: Some(32),
        summary_mode: ReasoningSummaryMode::Summary,
    };
    let request = GenerateRequest::new(vec![
        Message::user_text("hello").expect("user message should validate"),
        continued.clone(),
    ])
    .expect("continued request should validate")
    .with_tools(vec![ProviderToolSpec::Function(tool.clone())])
    .expect("function tool should attach")
    .with_response_format(ResponseFormat::JsonSchema(schema.clone()))
    .with_reasoning(reasoning.clone())
    .with_generation(generation.clone())
    .expect("generation controls should validate");
    let mapped = request
        .clone()
        .try_map_continuation(|value| Ok(value.len()))
        .expect("request continuation should map");
    assert_eq!(mapped.messages().len(), 2);
    assert_eq!(mapped.messages()[1].continuation(), Some(&12));
    assert_eq!(mapped.tools(), &[ProviderToolSpec::Function(tool)]);
    assert_eq!(
        mapped.response_format(),
        Some(&ResponseFormat::JsonSchema(schema))
    );
    assert_eq!(mapped.reasoning(), Some(&reasoning));
    assert_eq!(mapped.generation(), &generation);
    assert!(matches!(
        request
            .clone()
            .try_map_continuation::<usize>(|_| Err(SdkError::new(ErrorKind::InvalidState, Operation::ValidateInput, ErrorPhase::InputValidation).with_native_details(NativeErrorDetails { message: Some("map failed".into()), ..Default::default() }))),
        Err(error) if error.kind() == ErrorKind::InvalidState && error.native_details().message.as_deref() == Some("map failed")
    ));

    let citation = Citation::Url {
        url: "https://example.test/source".to_string(),
        title: Some("Source".to_string()),
        snippet: Some("fixture".to_string()),
        start_index: Some(1),
        end_index: Some(8),
    };
    let artifact = Artifact::Inline {
        media_type: "text/plain".to_string(),
        file_name: Some("result.txt".to_string()),
        description: Some("fixture artifact".to_string()),
        data: vec![1, 2, 3],
    };
    let hosted = HostedToolOutput::UrlContext {
        requested_url: None,
        retrieved_url: None,
        status: Some(HostedToolStatus::Completed),
        citations: vec![citation.clone()],
    };
    let response = GenerateResponse {
        model: model.clone(),
        message: continued,
        finish_reason: Some(FinishReason::Stop),
        citations: vec![citation.clone()],
        artifacts: vec![artifact.clone()],
        hosted_tool_outputs: vec![hosted.clone()],
        usage: Usage {
            input_tokens: Some(1),
            output_tokens: Some(2),
            reasoning_tokens: Some(3),
            cached_input_tokens: Some(4),
            total_tokens: Some(10),
        },
    };
    let mapped = response
        .clone()
        .try_map_continuation(|value| Ok(value.len()))
        .expect("response continuation should map");
    assert_eq!(mapped.model, model);
    assert_eq!(mapped.finish_reason, Some(FinishReason::Stop));
    assert_eq!(mapped.citations, vec![citation]);
    assert_eq!(mapped.artifacts, vec![artifact]);
    assert_eq!(mapped.hosted_tool_outputs, vec![hosted]);
    assert_eq!(mapped.usage, response.usage);
    assert_eq!(mapped.message.continuation(), Some(&12));
    assert!(
        response
            .clone()
            .try_map_continuation::<usize>(|_| Err(SdkError::new(
                ErrorKind::InvalidState,
                Operation::ValidateInput,
                ErrorPhase::InputValidation
            )
            .with_native_details(NativeErrorDetails {
                message: Some("map failed".into()),
                ..Default::default()
            })))
            .is_err()
    );

    let event = GenerateEvent::Completed(Box::new(response.clone()));
    let mapped = event
        .clone()
        .try_map_continuation(|value| Ok(value.len()))
        .expect("event continuation should map");
    assert!(matches!(
        mapped,
        GenerateEvent::Completed(output)
            if output.message.continuation() == Some(&12)
                && output.finish_reason == Some(FinishReason::Stop)
    ));
    assert!(
        event
            .try_map_continuation::<usize>(|_| Err(SdkError::new(
                ErrorKind::InvalidState,
                Operation::ValidateInput,
                ErrorPhase::InputValidation
            )
            .with_native_details(NativeErrorDetails {
                message: Some("map failed".into()),
                ..Default::default()
            })))
            .is_err()
    );

    let mut passthrough_mapper_called = false;
    let started = GenerateEvent::<String>::Started {
        model: model.clone(),
    }
    .try_map_continuation::<usize>(|_| {
        passthrough_mapper_called = true;
        Ok(0)
    })
    .expect("event without continuation should pass through");
    assert!(!passthrough_mapper_called);
    assert_eq!(
        started,
        GenerateEvent::Started {
            model: model.clone()
        }
    );

    let row = BatchInput::new("row-1", request).expect("batch row should validate");
    let batch = BatchSubmitRequest::Inline { rows: vec![row] };
    let mapped = batch
        .clone()
        .try_map_continuation(|value| Ok(value.len()))
        .expect("batch continuation should map");
    assert!(matches!(
        mapped,
        BatchSubmitRequest::Inline { rows }
            if rows[0].custom_id() == "row-1"
                && rows[0].request().messages()[1].continuation() == Some(&12)
    ));
    assert!(
        batch
            .try_map_continuation::<usize>(|_| Err(SdkError::new(
                ErrorKind::InvalidState,
                Operation::ValidateInput,
                ErrorPhase::InputValidation
            )
            .with_native_details(NativeErrorDetails {
                message: Some("map failed".into()),
                ..Default::default()
            })))
            .is_err()
    );

    let mut mapped_provider_file = false;
    let provider_file = BatchSubmitRequest::<String>::ProviderFile {
        file_id: "file-batch-1".to_string(),
    }
    .try_map_continuation::<usize>(|_| {
        mapped_provider_file = true;
        Ok(0)
    })
    .expect("provider-file mapping should preserve the opaque source");
    assert!(!mapped_provider_file);
    assert!(matches!(
        provider_file,
        BatchSubmitRequest::ProviderFile { file_id } if file_id == "file-batch-1"
    ));

    let result = BatchResult::Succeeded {
        custom_id: "row-1".to_string(),
        output: Box::new(response),
    };
    let mapped = result
        .clone()
        .try_map_continuation(|value| Ok(value.len()))
        .expect("batch result continuation should map");
    assert!(matches!(
        mapped,
        BatchResult::Succeeded { custom_id, output }
            if custom_id == "row-1" && output.message.continuation() == Some(&12)
    ));
    assert!(
        result
            .try_map_continuation::<usize>(|_| Err(SdkError::new(
                ErrorKind::InvalidState,
                Operation::ValidateInput,
                ErrorPhase::InputValidation
            )
            .with_native_details(NativeErrorDetails {
                message: Some("map failed".into()),
                ..Default::default()
            })))
            .is_err()
    );
}

#[test]
fn passive_model_pages_and_usage_values_remain_honest_dtos() {
    let summary = ModelSummary {
        id: ModelId::new("model-a").expect("model id should validate"),
        display_name: Some("Model A".to_string()),
        description: None,
        context_window_tokens: Some(128_000),
    };
    let page = ModelPage {
        models: vec![summary.clone()],
        next_cursor: None,
    };
    assert_eq!(page.models, vec![summary]);
    let request = ModelListRequest::default();
    assert!(request.limit().is_none());
    assert!(request.cursor().is_none());

    let usage = Usage {
        input_tokens: Some(2),
        output_tokens: Some(3),
        reasoning_tokens: None,
        cached_input_tokens: Some(1),
        total_tokens: Some(5),
    };
    assert_eq!(usage.total_tokens, Some(5));
}

#[test]
fn credential_and_error_debug_output_redacts_sensitive_values() {
    let key =
        provider_sdk::config::ApiKey::new("secret-test-key").expect("fixture key should validate");
    assert!(!format!("{key:?}").contains("secret-test-key"));

    let error = SdkError::new(
        ErrorKind::Transport,
        Operation::Generate,
        ErrorPhase::RequestSend,
    )
    .with_http_status(401)
    .with_native_details(NativeErrorDetails {
        message: Some("authorization=secret-test-key".into()),
        url: Some("https://example.test/?key=secret-test-key".into()),
        ..Default::default()
    });
    let debug = format!("{error:?}");
    assert!(!debug.contains("secret-test-key"));
    assert!(debug.contains("<redacted>"));
}
