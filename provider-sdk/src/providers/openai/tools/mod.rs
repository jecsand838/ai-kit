use std::collections::BTreeSet;

use serde_json::{Value, json};

use crate::SdkError;
use crate::error::ValidationViolation;
use crate::hosted::HostedTool;

pub(crate) fn render_hosted_tool(tool: &HostedTool) -> Result<Value, SdkError> {
    match tool {
        HostedTool::WebSearch => Ok(json!({"type": "web_search"})),
        HostedTool::FileSearch { store_ids } => {
            if store_ids.is_empty()
                || store_ids.iter().any(|id| id.trim().is_empty())
                || store_ids.iter().collect::<BTreeSet<_>>().len() != store_ids.len()
            {
                return Err(SdkError::invalid_input(
                    ValidationViolation::InvalidValue {
                        field: "tools.file_search.store_ids",
                        rule: "nonempty collection of unique nonblank IDs",
                    },
                    "OpenAI file search requires unique nonblank store ids",
                ));
            }
            Ok(json!({
                "type": "file_search",
                "vector_store_ids": store_ids,
            }))
        }
        HostedTool::CodeExecution => Ok(json!({
            "type": "code_interpreter",
            "container": {"type": "auto"},
        })),
        HostedTool::UrlContext => Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "tools.url_context",
                rule: "URL context unavailable",
            },
            "OpenAI URL context",
        )),
    }
}
