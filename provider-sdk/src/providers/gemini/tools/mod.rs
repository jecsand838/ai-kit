use std::collections::BTreeSet;

use serde_json::{Map, Value, json};

use crate::SdkError;
use crate::core::HostedToolMode;
use crate::error::ValidationViolation;
use crate::generation::ProviderToolSpec;
use crate::hosted::HostedTool;
use crate::models::ModelId;

pub(crate) fn render(
    model: &ModelId,
    specs: &[ProviderToolSpec],
    _mode: HostedToolMode,
) -> Result<(Vec<Value>, Map<String, Value>), SdkError> {
    let hosted = specs
        .iter()
        .filter_map(ProviderToolSpec::hosted_tool)
        .collect::<Vec<_>>();
    if hosted.is_empty() {
        return Ok((Vec::new(), Map::new()));
    }
    if !supports_hosted(model) {
        return Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "tools.hosted",
                rule: "hosted tools unavailable for selected model",
            },
            format!("Gemini hosted tools for model `{}`", model.as_str()),
        ));
    }
    if hosted.len() > 1
        && hosted
            .iter()
            .any(|tool| matches!(tool, HostedTool::FileSearch { .. }))
    {
        return Err(SdkError::invalid_input(
            ValidationViolation::Incompatible {
                field: "tools.file_search",
                other: "tools.hosted",
            },
            "Gemini file search cannot be combined with another hosted tool",
        ));
    }
    let mut tools = Vec::new();
    for tool in hosted {
        tools.push(match tool {
            HostedTool::WebSearch => json!({"google_search": {}}),
            HostedTool::UrlContext => json!({"url_context": {}}),
            HostedTool::CodeExecution => json!({"code_execution": {}}),
            HostedTool::FileSearch { store_ids } => {
                validate_store_ids(store_ids)?;
                json!({"fileSearch": {"file_search_store_names": store_ids}})
            }
        });
    }
    Ok((tools, Map::new()))
}

fn validate_store_ids(values: &[String]) -> Result<(), SdkError> {
    let mut unique = BTreeSet::new();
    if values.is_empty()
        || values
            .iter()
            .any(|value| value.trim().is_empty() || !unique.insert(value))
    {
        return Err(SdkError::invalid_input(
            ValidationViolation::InvalidValue {
                field: "tools.file_search.store_ids",
                rule: "nonempty collection of unique nonblank IDs",
            },
            "Gemini file search requires unique nonblank store IDs",
        ));
    }
    Ok(())
}

fn supports_hosted(model: &ModelId) -> bool {
    matches!(
        model.as_str(),
        "models/gemini-4-argon"
            | "models/gemini-3.8-flash"
            | "models/gemini-3.1-pro-preview"
            | "models/gemini-3-flash-preview"
            | "models/gemini-2.5-pro"
            | "models/gemini-2.5-flash"
            | "models/gemini-2.5-flash-lite"
    )
}
