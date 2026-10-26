use serde_json::{Value, json};

use crate::SdkError;
use crate::error::ValidationViolation;
use crate::hosted::HostedTool;

pub(crate) struct AnthropicHostedPlanner;

impl AnthropicHostedPlanner {
    pub(crate) const fn new() -> Self {
        Self
    }

    pub(crate) fn validate<'a>(
        &self,
        model: &str,
        tools: impl IntoIterator<Item = &'a HostedTool>,
    ) -> Result<(), SdkError> {
        let mut has_hosted = false;
        for tool in tools {
            has_hosted = true;
            if matches!(tool, HostedTool::FileSearch { .. }) {
                return Err(SdkError::unsupported(
                    ValidationViolation::Unsupported {
                        field: "tools.file_search",
                        rule: "hosted file search unavailable",
                    },
                    "Anthropic hosted file search",
                ));
            }
        }
        if has_hosted
            && !matches!(
                model,
                "claude-fable-5-1"
                    | "claude-opus-5-5"
                    | "claude-sonnet-5-5"
                    | "claude-haiku-5-5"
                    | "claude-fable-5"
                    | "claude-mythos-5"
                    | "claude-opus-5"
                    | "claude-sonnet-5"
                    | "claude-opus-4-8"
                    | "claude-opus-4-7"
                    | "claude-opus-4-6"
                    | "claude-sonnet-4-6"
                    | "claude-opus-4-5-20251101"
                    | "claude-sonnet-4-5-20250929"
                    | "claude-haiku-4-5"
                    | "claude-haiku-4-5-20251001"
            )
        {
            return Err(SdkError::unsupported(
                ValidationViolation::Unsupported {
                    field: "tools.hosted",
                    rule: "hosted tools unavailable for selected model",
                },
                format!("Anthropic hosted tools for exact model `{model}`"),
            ));
        }
        Ok(())
    }

    pub(crate) fn render_declaration(&self, tool: &HostedTool) -> Result<Value, SdkError> {
        match tool {
            HostedTool::WebSearch => Ok(json!({
                "type": "web_search_20250305",
                "name": "web_search",
            })),
            HostedTool::UrlContext => Ok(json!({
                "type": "web_fetch_20250910",
                "name": "web_fetch",
            })),
            HostedTool::CodeExecution => Ok(json!({
                "type": "code_execution_20250825",
                "name": "code_execution",
            })),
            HostedTool::FileSearch { .. } => Err(SdkError::unsupported(
                ValidationViolation::Unsupported {
                    field: "tools.file_search",
                    rule: "hosted file search unavailable",
                },
                "Anthropic hosted file search",
            )),
        }
    }
}
