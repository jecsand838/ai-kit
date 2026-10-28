use serde_json::{Map, Value, json};

use crate::KitError;
use crate::core::{GenerateRequestView, filter_tools_for_choice};
use crate::error::ValidationViolation;
use crate::generation::ProviderToolSpec;
use crate::models::ModelId;

use super::{
    input::build_input_items,
    tools::{response_format_to_value, tool_choice_to_value, tool_spec_to_value},
};

impl super::super::models::OpenAiModel {
    pub(in crate::providers::openai) fn build_response_body(
        model: &ModelId,
        request: GenerateRequestView<'_>,
        stream: bool,
    ) -> Result<Value, KitError> {
        Self::validate_stateless_model(model)?;
        let mut body = Map::new();
        body.insert(
            "model".to_string(),
            Value::String(model.as_str().to_string()),
        );

        let input = build_input_items(model, request)?;
        if !input.is_empty() {
            body.insert("input".to_string(), Value::Array(input));
        }

        let tool_choice = request.generation.tool_choice();
        let selected_tools = filter_tools_for_choice(request.tools, tool_choice)?;
        Self::validate_response_request(model, request, &selected_tools, stream)?;
        let tools = selected_tools
            .iter()
            .map(tool_spec_to_value)
            .collect::<Result<Vec<_>, _>>()?;
        if !tools.is_empty() {
            body.insert("tools".to_string(), Value::Array(tools));
        }

        if let Some(format) = response_format_to_value(request.response_format)? {
            body.insert("text".to_string(), json!({"format": format}));
        }

        Self::insert_response_reasoning_fields(&mut body, model, request)?;
        if let Some(value) = request.generation.temperature() {
            body.insert("temperature".to_string(), json!(value));
        }
        if let Some(value) = request.generation.top_p() {
            body.insert("top_p".to_string(), json!(value));
        }
        if request.generation.top_k().is_some() {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "generation.top_k",
                    rule: "top-k unavailable",
                },
                "OpenAI top_k generation control",
            ));
        }
        if let Some(value) = request.generation.max_output_tokens() {
            body.insert("max_output_tokens".to_string(), json!(value.get()));
        }
        if let Some(choice) = tool_choice {
            body.insert(
                "tool_choice".to_string(),
                tool_choice_to_value(choice, &selected_tools)?,
            );
        }

        if stream {
            body.insert("stream".to_string(), Value::Bool(true));
        }

        let mut include = vec![Value::String("reasoning.encrypted_content".to_owned())];
        let (mut web_search, mut code_execution, mut file_search) = (false, false, false);
        for tool in &selected_tools {
            match tool {
                ProviderToolSpec::Hosted(crate::hosted::HostedTool::WebSearch) => web_search = true,
                ProviderToolSpec::Hosted(crate::hosted::HostedTool::CodeExecution) => {
                    code_execution = true;
                }
                ProviderToolSpec::Hosted(crate::hosted::HostedTool::FileSearch { .. }) => {
                    file_search = true;
                }
                ProviderToolSpec::Function(_) | ProviderToolSpec::Hosted(_) => {}
            }
        }
        if web_search {
            include.push(Value::String("web_search_call.action.sources".to_owned()));
        }
        if code_execution {
            include.push(Value::String("code_interpreter_call.outputs".to_owned()));
        }
        if file_search {
            include.push(Value::String("file_search_call.results".to_owned()));
        }
        body.insert("include".to_string(), Value::Array(include));

        body.insert("store".to_string(), Value::Bool(false));

        Ok(Value::Object(body))
    }

    fn validate_response_request(
        model: &ModelId,
        request: GenerateRequestView<'_>,
        tools: &[ProviderToolSpec],
        stream: bool,
    ) -> Result<(), KitError> {
        let exact_reasoning_model = matches!(
            model.as_str(),
            "gpt-6-astra"
                | "gpt-6.1-sol"
                | "gpt-6-sol"
                | "gpt-6-luna"
                | "gpt-5.6"
                | "gpt-5.6-sol"
                | "gpt-5.6-terra"
                | "gpt-5.6-luna"
                | "gpt-5.6-cyber"
                | "gpt-5.5"
                | "gpt-5.5-2026-04-23"
                | "gpt-5.5-pro"
                | "gpt-5.5-pro-2026-04-23"
                | "gpt-5.4"
                | "gpt-5.4-2026-03-05"
                | "gpt-5.4-pro"
                | "gpt-5.4-pro-2026-03-05"
                | "gpt-5.4-mini"
                | "gpt-5.4-mini-2026-03-17"
                | "gpt-5.4-nano"
                | "gpt-5.4-nano-2026-03-17"
        );
        let exact_model = exact_reasoning_model
            || matches!(
                model.as_str(),
                "gpt-4.1" | "gpt-4.1-mini" | "gpt-4.1-nano" | "chat-latest"
            );
        let generation = request.generation;
        let has_generation_overrides = generation.temperature().is_some()
            || generation.top_p().is_some()
            || generation.top_k().is_some()
            || generation.max_output_tokens().is_some()
            || !generation.stop_sequences().is_empty()
            || generation.presence_penalty().is_some()
            || generation.frequency_penalty().is_some()
            || generation.seed().is_some();
        if (!tools.is_empty()
            || request.response_format.is_some()
            || request.reasoning.is_some()
            || has_generation_overrides)
            && !exact_model
        {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "generation",
                    rule: "selected model does not support advanced controls",
                },
                format!("advanced OpenAI generation controls for exact model `{model}`"),
            ));
        }

        if !generation.stop_sequences().is_empty() {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "generation.stop_sequences",
                    rule: "stop sequences unavailable",
                },
                "OpenAI Responses stop-sequence control",
            ));
        }
        if generation.presence_penalty().is_some() {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "generation.presence_penalty",
                    rule: "presence penalty unavailable",
                },
                "OpenAI Responses presence-penalty control",
            ));
        }
        if generation.frequency_penalty().is_some() {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "generation.frequency_penalty",
                    rule: "frequency penalty unavailable",
                },
                "OpenAI Responses frequency-penalty control",
            ));
        }
        if generation.seed().is_some() {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "generation.seed",
                    rule: "seed unavailable",
                },
                "OpenAI Responses seed control",
            ));
        }

        if stream && matches!(model.as_str(), "gpt-5.5-pro" | "gpt-5.5-pro-2026-04-23") {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "generation.stream",
                    rule: "streaming unavailable for selected model",
                },
                format!("OpenAI streaming for exact model `{model}`"),
            ));
        }

        if request.response_format.is_some()
            && matches!(model.as_str(), "gpt-5.4-pro" | "gpt-5.4-pro-2026-03-05")
        {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "response_format",
                    rule: "structured output unavailable for selected model",
                },
                format!("OpenAI structured output for exact model `{model}`"),
            ));
        }

        if tools.iter().any(|tool| {
            matches!(
                tool,
                ProviderToolSpec::Hosted(crate::hosted::HostedTool::CodeExecution)
            )
        }) && matches!(model.as_str(), "gpt-5.4-pro" | "gpt-5.4-pro-2026-03-05")
        {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "tools.code_execution",
                    rule: "code execution unavailable for selected model",
                },
                format!("OpenAI code execution for exact model `{model}`"),
            ));
        }

        if request.reasoning.is_some() && !exact_reasoning_model {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning",
                    rule: "reasoning unavailable for selected model",
                },
                format!("OpenAI reasoning controls for exact model `{model}`"),
            ));
        }

        if matches!(
            model.as_str(),
            "gpt-5.4-pro" | "gpt-5.4-pro-2026-03-05" | "gpt-5.5-pro" | "gpt-5.5-pro-2026-04-23"
        ) && request.reasoning.is_some_and(|reasoning| {
            matches!(
                reasoning.effort,
                Some(
                    crate::generation::ReasoningEffort::Minimal
                        | crate::generation::ReasoningEffort::Low
                )
            )
        }) {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning.effort",
                    rule: "selected model requires at least medium effort",
                },
                "OpenAI Pro model reasoning below medium effort",
            ));
        }

        if generation.temperature().is_some() || generation.top_p().is_some() {
            let supports_sampling = matches!(
                model.as_str(),
                "gpt-4.1" | "gpt-4.1-mini" | "gpt-4.1-nano" | "chat-latest"
            ) || (matches!(model.as_str(), "gpt-6-luna" | "gpt-6-sol")
                && request.reasoning.is_some_and(|reasoning| {
                    reasoning.effort == Some(crate::generation::ReasoningEffort::Minimal)
                }))
                || (matches!(
                    model.as_str(),
                    "gpt-5.4"
                        | "gpt-5.4-2026-03-05"
                        | "gpt-5.4-mini"
                        | "gpt-5.4-mini-2026-03-17"
                        | "gpt-5.4-nano"
                        | "gpt-5.4-nano-2026-03-17"
                ) && request.reasoning.is_none_or(|reasoning| {
                    reasoning.effort.is_none_or(|effort| {
                        matches!(effort, crate::generation::ReasoningEffort::Minimal)
                    })
                }));
            if !supports_sampling {
                return Err(KitError::unsupported(
                    ValidationViolation::Unsupported {
                        field: "generation.sampling",
                        rule: "sampling controls unavailable for selected model and reasoning effort",
                    },
                    format!(
                        "OpenAI temperature/top_p controls for exact model `{model}` and reasoning effort"
                    ),
                ));
            }
        }
        Ok(())
    }
}
