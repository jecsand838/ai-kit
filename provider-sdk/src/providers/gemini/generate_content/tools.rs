use serde_json::{Map, Value};

use crate::SdkError;
use crate::core::{GenerateRequestView, HostedToolMode, selected_tools_for_choice};
use crate::generation::{AllowedToolMode, ProviderToolSpec, ToolChoice};
use crate::models::ModelId;
use crate::providers::gemini::tools;

pub(crate) fn build_tools(
    model: &ModelId,
    request: GenerateRequestView<'_>,
    mode: HostedToolMode,
) -> Result<(Vec<Value>, Option<Value>), SdkError> {
    let (selected, selected_names) =
        selected_tools_for_choice(request.tools, request.generation.tool_choice())?;
    let (mut rendered, mut tool_config) = tools::render(model, &selected, mode)?;
    let declarations = selected
        .iter()
        .filter_map(|spec| match spec {
            ProviderToolSpec::Function(tool) => Some(tool),
            ProviderToolSpec::Hosted(_) => None,
        })
        .map(|tool| {
            let mut declaration = Map::new();
            declaration.insert("name".to_string(), Value::String(tool.name().to_string()));
            if let Some(description) = tool.description() {
                declaration.insert(
                    "description".to_string(),
                    Value::String(description.to_string()),
                );
            }
            declaration.insert(
                "parametersJsonSchema".to_string(),
                tool.input_schema().schema().clone(),
            );
            Value::Object(declaration)
        })
        .collect::<Vec<_>>();
    if !declarations.is_empty() {
        rendered.push(Value::Object(
            [(
                "functionDeclarations".to_string(),
                Value::Array(declarations),
            )]
            .into_iter()
            .collect(),
        ));
    }
    if let Some(choice) = request.generation.tool_choice()
        && rendered
            .iter()
            .any(|tool| tool.get("functionDeclarations").is_some())
    {
        let mut config = Map::new();
        config.insert(
            "mode".to_string(),
            Value::String(
                match choice {
                    ToolChoice::Auto => "AUTO",
                    ToolChoice::None => "NONE",
                    ToolChoice::Required | ToolChoice::Any | ToolChoice::Tool { .. } => "ANY",
                    ToolChoice::Allowed {
                        mode: AllowedToolMode::Auto,
                        ..
                    } => "VALIDATED",
                    ToolChoice::Allowed {
                        mode: AllowedToolMode::Required,
                        ..
                    } => "ANY",
                }
                .to_string(),
            ),
        );
        if let Some(names) = selected_names {
            config.insert(
                "allowedFunctionNames".to_string(),
                Value::Array(names.into_iter().map(Value::String).collect()),
            );
        }
        tool_config.insert("functionCallingConfig".to_string(), Value::Object(config));
    }
    Ok((
        rendered,
        (!tool_config.is_empty()).then_some(Value::Object(tool_config)),
    ))
}
