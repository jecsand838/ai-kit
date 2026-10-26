use tool_sdk::{ToolError, execution::ToolResult};

/// An independent expected result, loaded before any connection is attempted.
pub(crate) struct ExpectedToolResult(ToolResult);

impl ExpectedToolResult {
    pub(crate) fn parse(json: &str) -> Result<Self, ToolError> {
        let result: ToolResult =
            serde_json::from_str(json).map_err(|_| ToolError::InvalidConfiguration {
                message: "TEST_TOOL_SDK_MCP_EXPECTED_RESULT_JSON must contain a ToolResult"
                    .to_owned(),
            })?;
        if result.is_error || result.content.is_empty() {
            return Err(ToolError::InvalidConfiguration {
                message: "strict-live expects a nonempty successful ToolResult".to_owned(),
            });
        }
        Ok(Self(result))
    }

    pub(crate) fn verify(&self, actual: &ToolResult) -> Result<(), ToolError> {
        if actual != &self.0 {
            return Err(ToolError::Protocol {
                message:
                    "configured strict-live MCP result differs from the independent expected result"
                        .to_owned(),
            });
        }
        Ok(())
    }
}
