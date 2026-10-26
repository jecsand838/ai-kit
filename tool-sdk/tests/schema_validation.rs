pub mod support;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tool_sdk::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::{ToolInfo, ToolName},
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    interaction::{ToolInteractionCapabilities, ToolInteractionResponse},
    local::ToolRegistration,
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

struct OutputSchemaCase;

impl OutputSchemaCase {
    async fn invoke(result: ToolResult, declared: bool) -> Result<ToolHandlerOutput, ToolError> {
        let schema = declared.then(|| {
            JsonSchema::strict(json!({
                "type":"object", "required":["answer"],
                "properties":{"answer":{"type":"integer"}}, "additionalProperties":false
            }))
        });
        Self::with_schema(result, schema).await
    }

    async fn with_schema(
        result: ToolResult,
        schema: Option<JsonSchema>,
    ) -> Result<ToolHandlerOutput, ToolError> {
        let mut info = ToolInfo::new(
            ToolName::new("output").unwrap(),
            JsonSchema::strict(json!({"type":"object"})),
        );
        info.output_schema = schema;
        let tools = Toolset::local(vec![ToolRegistration::function(info, move |_| {
            let result = result.clone();
            async move { Ok(result) }
        })])
        .unwrap();
        let mut handler = tools
            .tool_handler(ToolName::new("output").unwrap())
            .await
            .unwrap();
        let outcome = handler
            .step(ToolHandlerInput::Invoke(ToolInvocation {
                call_id: None,
                arguments: json!({}),
            }))
            .await;
        drop(handler);
        tools
            .shutdown()
            .await
            .expect("local schema fixture shutdown");
        outcome
    }
}

#[tokio::test]
async fn output_schema_requires_structured_content_for_success() {
    let result = OutputSchemaCase::invoke(ToolResult::text("done"), true).await;
    assert!(
        matches!(result, Err(ToolError::InvalidOutput { .. })),
        "{result:?}"
    );
}

#[tokio::test]
async fn output_schema_accepts_valid_and_rejects_invalid_structured_content() {
    let result = OutputSchemaCase::invoke(ToolResult::json(json!({"answer":42})), true).await;
    assert!(
        matches!(result, Ok(ToolHandlerOutput::Completed(_))),
        "{result:?}"
    );
    let result = OutputSchemaCase::invoke(ToolResult::json(json!({"answer":"wrong"})), true).await;
    assert!(
        matches!(result, Err(ToolError::InvalidOutput { .. })),
        "{result:?}"
    );
    let result = OutputSchemaCase::invoke(ToolResult::json(serde_json::Value::Null), true).await;
    assert!(
        matches!(result, Err(ToolError::InvalidOutput { .. })),
        "explicit null must be validated against the object schema: {result:?}"
    );
}

#[tokio::test]
async fn output_schema_preserves_model_visible_errors_and_schema_free_text() {
    for (result, declared) in [
        (ToolResult::text("failure").with_error(true), true),
        (ToolResult::text("plain"), false),
    ] {
        let expected = result.clone();
        let outcome = OutputSchemaCase::invoke(result, declared).await;
        assert_eq!(outcome.unwrap(), ToolHandlerOutput::Completed(expected));
    }
}

#[tokio::test]
async fn nullable_output_schema_distinguishes_missing_from_explicit_null() {
    let schema = JsonSchema::strict(json!({"type":["object", "null"]}));
    let missing =
        OutputSchemaCase::with_schema(ToolResult::text("missing"), Some(schema.clone())).await;
    let null =
        OutputSchemaCase::with_schema(ToolResult::json(serde_json::Value::Null), Some(schema))
            .await;
    assert!(
        matches!(missing, Err(ToolError::InvalidOutput { .. })),
        "{missing:?}"
    );
    assert_eq!(
        null.unwrap(),
        ToolHandlerOutput::Completed(ToolResult::json(serde_json::Value::Null))
    );
}

#[tokio::test]
async fn typed_arguments_report_the_deserialization_path() {
    let toolset = Toolset::local(vec![
        ToolRegistration::typed("echo", |args: EchoArgs| async move {
            Ok(EchoOutput {
                echoed: args.message,
            })
        })
        .unwrap(),
    ])
    .unwrap();
    let error = support::invoke_tool(&toolset, "echo", None, json!({"message":42}))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ToolError::InvalidArguments { issues, .. }
            if issues.iter().any(|issue| issue.path.contains("message"))
    ));
}

#[test]
fn schema_values_and_validation_issues_remain_source_neutral() {
    let strict = JsonSchema::strict(json!({"type":"object"}));
    assert_eq!(strict.strictness, SchemaStrictness::Strict);
    let issue = ValidationIssue {
        path: "/name".to_owned(),
        message: "expected string".to_owned(),
    };
    assert_eq!(issue.path, "/name");
}

#[tokio::test]
async fn form_schema_validation_rejects_non_object_content() {
    let mut info = ToolInfo::new(
        ToolName::new("profile").unwrap(),
        JsonSchema::strict(json!({"type":"object"})),
    );
    info.interactions = ToolInteractionCapabilities {
        form: true,
        form_schema_validation: true,
        text: false,
        multiline_text: false,
    };
    let toolset = Toolset::local(vec![ToolRegistration::interactive(
        info,
        |_invocation, context| async move {
            let profile = context
                .request_form(
                    "profile",
                    json!({"type":"object","required":["name"]}),
                    true,
                )
                .await?;
            Ok(ToolResult::json(profile))
        },
    )])
    .unwrap();
    let mut handler = toolset
        .tool_handler(ToolName::new("profile").unwrap())
        .await
        .unwrap();
    let ToolHandlerOutput::Interaction(request) = handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: None,
            arguments: json!({}),
        }))
        .await
        .unwrap()
    else {
        panic!("expected form request");
    };
    assert!(matches!(
        handler
            .step(ToolHandlerInput::Interaction(
                ToolInteractionResponse::submit(request.id, json!("wrong"))
            ))
            .await,
        Err(ToolError::InvalidInteraction { .. })
    ));
}
