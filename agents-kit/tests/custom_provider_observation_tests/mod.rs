use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use providers_kit::config::{OllamaConfig, ProviderKind, RetryPolicy, TransportConfig};
use providers_kit::error::{ErrorKind, ErrorPhase, NativeErrorDetails, Operation};
use providers_kit::generation::{GenerateEvent, GenerateEventStream, GenerateRequest, Message};
use providers_kit::{
    KitError, Model, ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig,
};
use tools_kit::declaration::ToolName;
use tools_kit::execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation};
use tools_kit::{ToolHandlerBackend, ToolsetBackend};

use crate::caller_shell::{CallerShell, CallerShellObserver};
use crate::custom_provider_observation::{
    ConfiguredProvider, GenerationDiagnostics, ObservedModel, ObservedProvider, ScenarioStage,
};

#[test]
fn custom_diagnostic_redacts_native_payloads_and_preserves_transport_indicators() {
    let secret = "do-not-render-private-credential";
    let error = KitError::new(
        ErrorKind::Transport,
        Operation::Generate,
        ErrorPhase::RequestSend,
    )
    .with_request_target(format!("https://user:{secret}@private.example/{secret}"))
    .with_native_details(NativeErrorDetails {
        message: Some(format!("TLS handshake failed: {secret}")),
        body: Some(format!("{{\"signature\":\"{secret}\"}}")),
        code: Some(secret.into()),
        error_type: Some(secret.into()),
        request_id: Some(secret.into()),
        url: Some(format!(
            "https://user:{secret}@private.example/?key={secret}"
        )),
        reported_http_status: Some(503),
        rpc_code: Some(14),
        ..NativeErrorDetails::default()
    })
    .with_timeout(Duration::from_secs(7))
    .with_cause(
        KitError::new(
            ErrorKind::Transport,
            Operation::Unspecified,
            ErrorPhase::RequestSend,
        )
        .with_native_details(NativeErrorDetails {
            message: Some(format!("connection reset by peer: {secret}")),
            ..NativeErrorDetails::default()
        }),
    );
    let diagnostics = Arc::new(GenerationDiagnostics::new(Arc::new(
        CallerShellObserver::default(),
    )));
    diagnostics.enter(ScenarioStage::Shell);
    let configured = ConfiguredProvider::new(
        &ProviderKind::Ollama(OllamaConfig::local()),
        Duration::from_secs(180),
        Duration::from_secs(15),
    );
    let probe = diagnostics.begin(
        &ModelId::new("models/exact-test-model").unwrap(),
        configured,
    );
    let evidence = probe.failure(&error);
    let encoded = serde_json::to_string(&evidence).unwrap();
    assert!(!encoded.contains(secret));
    assert!(!encoded.contains("private.example"));
    assert!(!encoded.contains("handshake failed"));
    assert_eq!(evidence["model"], "models/exact-test-model");
    assert_eq!(evidence["stage"], "Shell");
    assert_eq!(evidence["provider_kind"], "ollama");
    assert_eq!(evidence["configured_request_timeout_ms"], 180_000);
    assert_eq!(evidence["configured_connect_timeout_ms"], 15_000);
    assert_eq!(evidence["cause_chain"][0]["timeout_ms"], 7_000);
    assert_eq!(
        evidence["cause_chain"][1]["timeout_ms"],
        serde_json::Value::Null
    );
    assert_eq!(error.timeout(), Some(Duration::from_secs(7)));
    assert_eq!(
        evidence["cause_chain"][0]["native_message_indicators"],
        serde_json::json!(["tls"])
    );
    assert_eq!(
        evidence["cause_chain"][1]["native_message_indicators"],
        serde_json::json!(["connection_reset"])
    );
    assert_eq!(evidence["cause_chain"][0]["reported_http_status"], 503);
    assert_eq!(evidence["cause_chain"][0]["rpc_code"], 14);
    assert_eq!(
        evidence["cause_chain"][0]["http_status"],
        serde_json::Value::Null
    );
    assert_eq!(error.native_details().request_id.as_deref(), Some(secret));
}

#[test]
fn custom_diagnostic_bounds_unicode_causes_and_snapshots_stage_per_generation() {
    let message = format!("{}🦀connection refused", "x".repeat(2047));
    let mut error = KitError::new(
        ErrorKind::Transport,
        Operation::Generate,
        ErrorPhase::RequestSend,
    )
    .with_native_details(NativeErrorDetails {
        message: Some(message),
        ..NativeErrorDetails::default()
    });
    for _ in 0..10 {
        error = KitError::new(
            ErrorKind::Transport,
            Operation::Generate,
            ErrorPhase::RequestSend,
        )
        .with_native_details(NativeErrorDetails {
            message: Some(format!("{}🦀connection refused", "x".repeat(2047))),
            ..NativeErrorDetails::default()
        })
        .with_cause(error);
    }
    let diagnostics = Arc::new(GenerationDiagnostics::new(Arc::new(
        CallerShellObserver::default(),
    )));
    let model = ModelId::new("models/exact-test-model").unwrap();
    let configured = ConfiguredProvider::new(
        &ProviderKind::Ollama(OllamaConfig::local()),
        Duration::from_secs(180),
        Duration::from_secs(15),
    );
    let original = diagnostics.begin(&model, configured);
    for stage in [
        ScenarioStage::PlainGeneration,
        ScenarioStage::FunctionContinuation,
        ScenarioStage::ParallelTools,
        ScenarioStage::CallerCancellation,
        ScenarioStage::Shell,
        ScenarioStage::Approval,
        ScenarioStage::RuntimeLoss,
        ScenarioStage::Streaming,
    ] {
        diagnostics.enter(stage);
    }
    let second = diagnostics.begin(&model, configured);
    let first_evidence = original.failure(&error);
    let second_evidence = second.failure(&error);
    assert_eq!(first_evidence["stage"], "Setup");
    assert_eq!(second_evidence["stage"], "Streaming");
    assert_eq!(first_evidence["generation_ordinal"], 1);
    assert_eq!(second_evidence["generation_ordinal"], 2);
    assert_eq!(first_evidence["cause_chain"].as_array().unwrap().len(), 8);
    // The provider already bounds its causal depth. Preserve its truncation
    // evidence without claiming that this observer dropped another node.
    assert_eq!(first_evidence["cause_chain_truncated"], false);
    assert_eq!(
        first_evidence["cause_chain"][0]["diagnostics_truncated"],
        true
    );
    for node in first_evidence["cause_chain"].as_array().unwrap() {
        assert_eq!(node["message_inspection_truncated"], true);
        assert_eq!(node["native_message_indicators"], serde_json::json!([]));
    }
    assert!(serde_json::to_vec(&first_evidence).unwrap().len() <= 8 * 1024);
    let unsafe_model = ModelId::new("models/private?credential=secret").unwrap();
    let evidence = diagnostics.begin(&unsafe_model, configured).failure(&error);
    assert_eq!(evidence["model"], "<redacted-model>");
}

#[tokio::test]
async fn custom_diagnostic_provider_delegates_model_identity_and_shared_shutdown() {
    let kind = ProviderKind::Ollama(OllamaConfig::local());
    let request_timeout = Duration::from_secs(180);
    let connect_timeout = Duration::from_secs(15);
    let configured = ConfiguredProvider::new(&kind, request_timeout, connect_timeout);
    let provider = Provider::from_config(ProviderConfig::new(
        kind,
        TransportConfig::new(
            request_timeout,
            connect_timeout,
            Duration::from_secs(60),
            RetryPolicy::default(),
        )
        .unwrap(),
    ))
    .unwrap();
    let diagnostics = Arc::new(GenerationDiagnostics::new(Arc::new(
        CallerShellObserver::default(),
    )));
    let observed = ObservedProvider::new(provider.clone(), diagnostics, configured);
    let id = ModelId::new("exact-uncontacted-model").unwrap();
    let direct = provider.model(id.clone()).unwrap();
    let wrapped = observed.model(id.clone()).unwrap();
    assert_eq!(wrapped.model_id(), direct.model_id());
    let _continuation_identity: fn(
        <Model as ModelBackend>::Continuation,
    ) -> <ObservedModel as ModelBackend>::Continuation = |value| value;
    let _session_identity: fn(
        <Model as ModelBackend>::Session,
    ) -> <ObservedModel as ModelBackend>::Session = |value| value;
    observed.shutdown().await.unwrap();
    assert_eq!(provider.model(id).err().unwrap().kind(), ErrorKind::Closed);
    let request = GenerateRequest::new(vec![
        Message::user_text("Never sent: provider already closed").unwrap(),
    ])
    .unwrap();
    let expected = direct.generate(&request).await.unwrap_err();
    let actual = wrapped.generate(&request).await.unwrap_err();
    assert_eq!(actual.kind(), expected.kind());
    assert_eq!(actual.context(), expected.context());
    assert!(actual.native_details() == expected.native_details());
    let expected_stream = direct.generate_stream(&request).await.err().unwrap();
    let actual_stream = wrapped.generate_stream(&request).await.err().unwrap();
    assert_eq!(actual_stream.kind(), expected_stream.kind());
    assert_eq!(actual_stream.context(), expected_stream.context());
    assert!(actual_stream.native_details() == expected_stream.native_details());
}

#[tokio::test]
async fn custom_diagnostic_shell_counts_invocations_started_processes_and_reaping() {
    let root = std::env::temp_dir().canonicalize().unwrap();
    let observer = Arc::new(CallerShellObserver::default());
    let observed = CallerShell::toolset(
        root.clone(),
        Duration::from_secs(5),
        1024,
        Some(Arc::clone(&observer)),
    );
    let ordinary = CallerShell::toolset(root, Duration::from_secs(5), 1024, None);
    let mut ordinary_handler = ordinary
        .tool_handler(ToolName::new("shell").unwrap())
        .await
        .unwrap();
    let mut observed_handler = observed
        .tool_handler(ToolName::new("shell").unwrap())
        .await
        .unwrap();
    let invocation = ToolInvocation {
        call_id: Some("exact-call".into()),
        arguments: serde_json::json!({"command":"echo diagnostic", "cwd":"."}),
    };
    let expected = ordinary_handler
        .step(ToolHandlerInput::Invoke(invocation.clone()))
        .await;
    let actual = observed_handler
        .step(ToolHandlerInput::Invoke(invocation))
        .await;
    let completed = serde_json::to_value(observer.snapshot()).unwrap();
    let mut rejected_handler = observed
        .tool_handler(ToolName::new("shell").unwrap())
        .await
        .unwrap();
    let rejected = rejected_handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some("outside-call".into()),
            arguments: serde_json::json!({"command":"echo never-run", "cwd":".."}),
        }))
        .await;
    let rejected_counts = serde_json::to_value(observer.snapshot()).unwrap();
    ordinary.shutdown().await.unwrap();
    observed.shutdown().await.unwrap();
    assert_eq!(actual.unwrap(), expected.unwrap());
    assert_eq!(
        completed,
        serde_json::json!({"invoked":1,"started":1,"reaped":1,"returned":1})
    );
    assert!(matches!(rejected.unwrap(), ToolHandlerOutput::Completed(result) if result.is_error));
    assert_eq!(
        rejected_counts,
        serde_json::json!({"invoked":2,"started":1,"reaped":1,"returned":2})
    );
}

#[tokio::test]
async fn custom_diagnostic_stream_preserves_order_bytes_errors_and_drop() {
    let diagnostics = Arc::new(GenerationDiagnostics::new(Arc::new(
        CallerShellObserver::default(),
    )));
    let model = ModelId::new("exact-uncontacted-model").unwrap();
    let configured = ConfiguredProvider::new(
        &ProviderKind::Ollama(OllamaConfig::local()),
        Duration::from_secs(180),
        Duration::from_secs(15),
    );
    let error = KitError::new(
        ErrorKind::Transport,
        Operation::GenerateStream,
        ErrorPhase::Stream,
    )
    .with_native_details(NativeErrorDetails {
        message: Some("connection reset: private-token".into()),
        ..NativeErrorDetails::default()
    });
    let stream = GenerateEventStream::new(futures_util::stream::iter([
        Ok(GenerateEvent::Started {
            model: model.clone(),
        }),
        Ok(GenerateEvent::ContentDelta {
            delta: "  untouched\r\n🦀".into(),
        }),
        Err(error.clone()),
    ]));
    let mut observed = diagnostics.begin(&model, configured).observe_stream(stream);
    assert!(
        matches!(observed.next().await.unwrap().unwrap(), GenerateEvent::Started { model: actual } if actual == model)
    );
    assert!(
        matches!(observed.next().await.unwrap().unwrap(), GenerateEvent::ContentDelta { delta } if delta == "  untouched\r\n🦀")
    );
    let actual = observed.next().await.unwrap().unwrap_err();
    assert_eq!(actual.kind(), error.kind());
    assert_eq!(actual.context(), error.context());
    assert!(actual.native_details() == error.native_details());
    assert!(observed.next().await.is_none());

    // A pending source retains this sender; dropping the observer must drop it
    // immediately instead of handing source ownership to a forwarding task.
    let (sender, mut receiver) = tokio::sync::oneshot::channel::<()>();
    let source = futures_util::stream::once(async move {
        let _sender = sender;
        std::future::pending::<Result<GenerateEvent, KitError>>().await
    });
    let observed = diagnostics
        .begin(&model, configured)
        .observe_stream(GenerateEventStream::new(source));
    drop(observed);
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    ));
}
