use serde::Serialize;

use super::CodexDynamicToolSpec;
use crate::SdkError;

pub(super) struct CodexDynamicToolDeclarations {
    entries: Vec<CodexDynamicToolDeclaration>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum CodexDynamicToolDeclaration {
    Function(CodexDynamicFunction),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexDynamicFunction {
    name: String,
    description: String,
    input_schema: serde_json::Value,
}

impl CodexDynamicToolDeclarations {
    pub(super) fn from_specs(specs: &[CodexDynamicToolSpec]) -> Result<Self, SdkError> {
        let entries = specs
            .iter()
            .map(|spec| {
                CodexDynamicToolDeclaration::Function(CodexDynamicFunction {
                    name: spec.name.clone(),
                    description: spec.description.clone(),
                    input_schema: spec.input_schema.clone(),
                })
            })
            .collect();
        Ok(Self { entries })
    }

    pub(super) fn into_values(self) -> Result<Vec<serde_json::Value>, SdkError> {
        self.entries
            .into_iter()
            .map(|entry| {
                serde_json::to_value(entry).map_err(|_| {
                    SdkError::protocol("dynamic tool declaration could not be encoded")
                })
            })
            .collect()
    }
}
