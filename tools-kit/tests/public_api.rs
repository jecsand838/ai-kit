use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tools_kit::{
    ToolError, ToolHandler, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::{ToolAnnotations, ToolIcon, ToolInfo, ToolName},
    execution::{ToolContent, ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    interaction::{
        ToolInteractionCapabilities, ToolInteractionId, ToolInteractionKind,
        ToolInteractionRequest, ToolInteractionResponse,
    },
    local::{ToolInvocationContext, ToolRegistration},
    mcp::{McpClientConfig, McpConnectionConfig},
    schema::{JsonSchema, SchemaStrictness, ValidationIssue},
};

#[derive(Deserialize, schemars::JsonSchema)]
struct EchoArgs {
    message: String,
}

#[derive(Serialize, schemars::JsonSchema)]
struct EchoOutput {
    echoed: String,
}

async fn macro_echo(args: EchoArgs) -> Result<EchoOutput, ToolError> {
    Ok(EchoOutput {
        echoed: args.message,
    })
}

#[test]
fn primary_handles_and_functional_module_types_have_expected_auto_traits() {
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_clone<T: Clone>() {}
    fn assert_handler_backend<T: ToolHandlerBackend>() {}
    fn assert_toolset_backend<T: ToolsetBackend>() {}

    assert_clone::<Toolset>();
    assert_send_sync::<Toolset>();
    assert_toolset_backend::<Toolset>();
    assert_send::<ToolHandler>();
    assert_handler_backend::<ToolHandler>();

    assert_send_sync::<ToolRegistration>();
    assert_send_sync::<ToolInvocationContext>();
    assert_send_sync::<McpConnectionConfig>();
    assert_send_sync::<McpClientConfig>();
    assert_send_sync::<ToolName>();
    assert_send_sync::<ToolInfo>();
    assert_send_sync::<ToolAnnotations>();
    assert_send_sync::<ToolIcon>();
    assert_send_sync::<ToolInvocation>();
    assert_send_sync::<ToolResult>();
    assert_send_sync::<ToolContent>();
    assert_send_sync::<ToolHandlerInput>();
    assert_send_sync::<ToolHandlerOutput>();
    assert_send_sync::<ToolInteractionCapabilities>();
    assert_send_sync::<ToolInteractionRequest>();
    assert_send_sync::<ToolInteractionResponse>();
    assert_send_sync::<ToolInteractionId>();
    assert_send_sync::<ToolInteractionKind>();
    assert_send_sync::<JsonSchema>();
    assert_send_sync::<SchemaStrictness>();
    assert_send_sync::<ValidationIssue>();
    assert_send_sync::<ToolError>();

    #[cfg(feature = "mcp-server")]
    assert_send::<tools_kit::mcp::McpServer>();
}

#[test]
fn mcp_configuration_debug_redacts_transport_secrets() {
    let stdio = McpClientConfig::new(McpConnectionConfig::Stdio {
        program: "fixture-mcp-server".to_owned(),
        args: vec![
            "--access-token".to_owned(),
            "STDIO_ARGUMENT_SECRET".to_owned(),
        ],
        cwd: Some("/fixture/WORKING_DIRECTORY_SECRET".into()),
        env: BTreeMap::from([(
            "FIXTURE_TOKEN".to_owned(),
            "STDIO_ENVIRONMENT_SECRET".to_owned(),
        )]),
    });
    let rendered = format!("{stdio:?}");
    assert!(rendered.contains("fixture-mcp-server"));
    assert!(rendered.contains("argument_count: 2"));
    assert!(rendered.contains("has_working_directory: true"));
    assert!(rendered.contains("environment_count: 1"));
    for secret in [
        "--access-token",
        "STDIO_ARGUMENT_SECRET",
        "FIXTURE_TOKEN",
        "STDIO_ENVIRONMENT_SECRET",
        "WORKING_DIRECTORY_SECRET",
    ] {
        assert!(!rendered.contains(secret));
    }

    let http = McpClientConfig::new(McpConnectionConfig::StreamableHttp {
        endpoint: "https://URL_USER_SECRET:URL_PASSWORD_SECRET@example.invalid:8443/PATH_SECRET?access_token=URL_QUERY_SECRET#URL_FRAGMENT_SECRET"
            .parse()
            .expect("fixture MCP endpoint should parse"),
        headers: BTreeMap::from([(
            "Authorization".to_owned(),
            "Bearer HTTP_HEADER_SECRET".to_owned(),
        )]),
    });
    let rendered = format!("{http:?}");
    assert!(rendered.contains("endpoint_scheme: \"https\""));
    assert!(rendered.contains("endpoint_host: Some(\"example.invalid\")"));
    assert!(rendered.contains("endpoint_port: Some(8443)"));
    assert!(rendered.contains("header_count: 1"));
    for secret in [
        "URL_USER_SECRET",
        "URL_PASSWORD_SECRET",
        "PATH_SECRET",
        "URL_QUERY_SECRET",
        "URL_FRAGMENT_SECRET",
        "Authorization",
        "HTTP_HEADER_SECRET",
    ] {
        assert!(!rendered.contains(secret));
    }
}

#[test]
fn tool_name_is_opaque_and_serializes_as_rendered_name() {
    let name = ToolName::new("demo_echo").expect("valid tool name should parse");
    assert_eq!(name.as_str(), "demo_echo");
    assert_eq!(serde_json::to_value(&name).unwrap(), json!("demo_echo"));
    let decoded: ToolName = serde_json::from_value(json!("demo_echo")).unwrap();
    assert_eq!(decoded, name);
}

#[tokio::test]
async fn macro_and_closure_registrations_execute_through_the_public_hierarchy() {
    let suffix = " captured".to_owned();
    let toolset = Toolset::local(vec![
        tools_kit::tool!(
            "macro_echo",
            description = "Echo via a function item.",
            macro_echo
        )
        .expect("function registration should validate"),
        tools_kit::tool!("macro_closure", move |args: EchoArgs| {
            let suffix = suffix.clone();
            async move {
                Ok(EchoOutput {
                    echoed: format!("{}{}", args.message, suffix),
                })
            }
        })
        .expect("closure registration should validate"),
    ])
    .expect("local Toolset should build");

    let tools = toolset.list_tools().await.expect("inventory should load");
    let macro_echo = tools
        .iter()
        .find(|tool| tool.name.as_str() == "macro_echo")
        .expect("function registration should be listed");
    assert_eq!(
        macro_echo.description.as_deref(),
        Some("Echo via a function item.")
    );

    let mut handler = toolset
        .tool_handler(ToolName::new("macro_closure").unwrap())
        .await
        .expect("handler should build");
    let output = handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some("call-1".to_owned()),
            arguments: json!({ "message": "hello" }),
        }))
        .await
        .expect("closure should execute");
    let ToolHandlerOutput::Completed(result) = output else {
        panic!("closure should complete without interaction");
    };
    assert_eq!(
        result.structured_content,
        Some(json!({ "echoed": "hello captured" }))
    );
}

#[test]
fn interaction_response_exposes_only_source_neutral_identity() {
    let id = ToolInteractionId::new();
    let response = ToolInteractionResponse::submit(id.clone(), json!({ "ok": true }));
    assert_eq!(response.interaction_id(), &id);
}
