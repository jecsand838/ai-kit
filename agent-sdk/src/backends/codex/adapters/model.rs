use serde::Deserialize;
use serde_json::Value;

use crate::{ModelInfo, ModelSelector, ReasoningEffort, SdkError};

const CODEX_MODEL_PAGE_LIMIT: usize = 100;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexModelListResponse {
    data: Vec<CodexModel>,
    next_cursor: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct CodexModelInventoryEntry {
    info: ModelInfo,
    default_reasoning_effort: ReasoningEffort,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexModel {
    model: String,
    display_name: String,
    hidden: bool,
    supported_reasoning_efforts: Vec<CodexReasoningEffortOption>,
    default_reasoning_effort: String,
    #[serde(rename = "supportsPersonality")]
    _supports_personality: bool,
    is_default: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexReasoningEffortOption {
    reasoning_effort: String,
}

impl CodexModelListResponse {
    pub(crate) fn decode(value: Value) -> Result<Self, SdkError> {
        let response = serde_json::from_value::<Self>(value)
            .map_err(|_| SdkError::protocol("model inventory response is malformed"))?;
        if response.data.len() > CODEX_MODEL_PAGE_LIMIT {
            return Err(SdkError::protocol(
                "model inventory page exceeds the requested bound",
            ));
        }
        if response
            .next_cursor
            .as_deref()
            .is_some_and(|cursor| cursor.is_empty() || cursor.trim() != cursor)
        {
            return Err(SdkError::protocol("model inventory cursor is malformed"));
        }
        Ok(response)
    }

    pub(crate) fn into_page(
        self,
    ) -> Result<(Vec<CodexModelInventoryEntry>, Option<String>), SdkError> {
        let models = self
            .data
            .into_iter()
            .map(CodexModel::into_inventory_entry)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((models, self.next_cursor))
    }
}

impl CodexModel {
    fn into_inventory_entry(self) -> Result<CodexModelInventoryEntry, SdkError> {
        if self.hidden {
            return Err(SdkError::protocol(
                "returned a hidden model to the selectable inventory",
            ));
        }
        let selector = ModelSelector::new(self.model)
            .map_err(|_| SdkError::protocol("model selector is malformed"))?;
        let reasoning_efforts = self
            .supported_reasoning_efforts
            .into_iter()
            .map(|option| {
                ReasoningEffort::new(option.reasoning_effort)
                    .map_err(|_| SdkError::protocol("reasoning effort is malformed"))
            })
            .collect::<Result<Vec<_>, SdkError>>()?;
        let default_reasoning_effort = ReasoningEffort::new(self.default_reasoning_effort)
            .map_err(|_| SdkError::protocol("default reasoning effort is malformed"))?;
        if !reasoning_efforts.contains(&default_reasoning_effort) {
            return Err(SdkError::protocol(
                "default reasoning effort is not selectable",
            ));
        }
        let info = ModelInfo::new(
            selector,
            Some(self.display_name),
            reasoning_efforts,
            self.is_default,
        )
        .map_err(|_| SdkError::protocol("model inventory entry is malformed"))?;
        Ok(CodexModelInventoryEntry {
            info,
            default_reasoning_effort,
        })
    }
}

impl CodexModelInventoryEntry {
    pub(crate) fn info(&self) -> &ModelInfo {
        &self.info
    }

    pub(crate) fn selector(&self) -> &ModelSelector {
        self.info.selector()
    }

    pub(crate) fn is_default(&self) -> bool {
        self.info.is_default()
    }

    pub(crate) fn default_reasoning_effort(&self) -> &ReasoningEffort {
        &self.default_reasoning_effort
    }

    pub(crate) fn into_info(self) -> ModelInfo {
        self.info
    }
}
