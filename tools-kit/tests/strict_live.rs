use std::{collections::BTreeMap, time::Duration};

mod live_assertions;
use live_assertions::ExpectedToolResult;

use tools_kit::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation},
    mcp::{McpClientConfig, McpConnectionConfig},
};

struct StrictMcpConfig {
    program: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    timeout: Duration,
    rendered_tool_name: String,
    arguments: serde_json::Value,
    expected: ExpectedToolResult,
}

impl StrictMcpConfig {
    fn from_env() -> Self {
        let program = Self::required("TEST_TOOLS_KIT_MCP_PROGRAM");
        let args = Self::json("TEST_TOOLS_KIT_MCP_ARGS_JSON");
        let env = Self::json("TEST_TOOLS_KIT_MCP_ENV_JSON");
        let timeout_secs = Self::required("TEST_TOOLS_KIT_MCP_TIMEOUT_SECS")
            .parse()
            .expect("TEST_TOOLS_KIT_MCP_TIMEOUT_SECS must be a positive integer");
        assert!(
            timeout_secs > 0,
            "TEST_TOOLS_KIT_MCP_TIMEOUT_SECS must be positive"
        );
        let rendered_tool_name = Self::required("TEST_TOOLS_KIT_MCP_TOOL_NAME");
        let arguments: serde_json::Value = Self::json("TEST_TOOLS_KIT_MCP_TOOL_ARGUMENTS_JSON");
        let expected =
            ExpectedToolResult::parse(&Self::required("TEST_TOOLS_KIT_MCP_EXPECTED_RESULT_JSON"))
                .expect("strict-live requires an independent successful expected ToolResult");
        assert!(
            arguments.is_object(),
            "TEST_TOOLS_KIT_MCP_TOOL_ARGUMENTS_JSON must be a JSON object"
        );
        Self {
            program,
            args,
            env,
            timeout: Duration::from_secs(timeout_secs),
            rendered_tool_name,
            arguments,
            expected,
        }
    }

    fn required(name: &str) -> String {
        let value = std::env::var(name)
            .unwrap_or_else(|_| panic!("missing required strict-live variable: {name}"))
            .trim()
            .to_owned();
        assert!(
            !value.is_empty(),
            "strict-live variable {name} must not be blank"
        );
        value
    }

    fn json<T: serde::de::DeserializeOwned>(name: &str) -> T {
        serde_json::from_str(&Self::required(name))
            .unwrap_or_else(|_| panic!("strict-live variable {name} contains invalid JSON"))
    }
}

#[tokio::test]
async fn independently_configured_stdio_mcp_inventory_call_and_shutdown() {
    let strict = StrictMcpConfig::from_env();
    let config = McpClientConfig::new(McpConnectionConfig::Stdio {
        program: strict.program,
        args: strict.args,
        cwd: None,
        env: strict.env,
    })
    .with_namespace("live")
    .expect("strict-live namespace should validate")
    .with_request_timeout(strict.timeout)
    .expect("strict-live timeout should validate");
    let toolset = tokio::time::timeout(strict.timeout, Toolset::mcp(config))
        .await
        .expect("independently configured MCP service must initialize before the caller deadline")
        .expect("independently configured MCP service must initialize");

    let proof = tokio::time::timeout(strict.timeout, async {
        let inventory = toolset.list_tools().await?;
        let name = inventory
            .iter()
            .find(|tool| tool.name.as_str() == strict.rendered_tool_name)
            .map(|tool| tool.name.clone())
            .ok_or_else(|| ToolError::InvalidConfiguration {
                message: "configured strict-live MCP tool was absent from inventory".to_owned(),
            })?;
        let mut handler = toolset.tool_handler(name).await?;
        match handler
            .step(ToolHandlerInput::Invoke(ToolInvocation {
                call_id: Some("tools-kit-strict-live".to_owned()),
                arguments: strict.arguments,
            }))
            .await?
        {
            ToolHandlerOutput::Completed(result) if !result.is_error => Ok(result),
            ToolHandlerOutput::Completed(_) | ToolHandlerOutput::Interaction(_) => {
                Err(ToolError::Protocol {
                    message: "configured strict-live MCP call did not complete successfully"
                        .to_owned(),
                })
            }
        }
    })
    .await;
    let shutdown = tokio::time::timeout(strict.timeout, toolset.shutdown()).await;

    let result = proof
        .expect("independently configured MCP call must finish before the caller deadline")
        .expect("independently configured MCP inventory/call proof must pass");
    shutdown
        .expect("independently configured MCP shutdown must finish before the caller deadline")
        .expect("independently configured MCP service must shut down");
    strict
        .expected
        .verify(&result)
        .expect("independently configured MCP call must return the exact expected result");
}
