use serde_json::{Map, Value, json};

use crate::KitError;
use crate::core::{GenerateRequestView, filter_tools_for_choice};
use crate::error::ValidationViolation;
use crate::generation::{
    ContentPart, GenerationContinuation, Message, NativeContinuation, ResponseFormat, Role,
};

use super::{
    attachments::{
        assistant_content_part_to_block, tool_result_content_part_to_block,
        user_content_part_to_block,
    },
    reasoning::ClaudeThinkingProfile,
    tools::{tool_choice_to_value, tool_spec_to_value},
};

const DEFAULT_MAX_OUTPUT_TOKENS: u32 = 1_024;

enum AnthropicMessageRole {
    User,
    Assistant,
}

impl AnthropicMessageRole {
    fn into_value(
        self,
        model: &str,
        message: &Message<GenerationContinuation>,
    ) -> Result<Value, KitError> {
        if let Some(continuation) = message.continuation() {
            if !matches!(self, Self::Assistant) {
                return Err(KitError::invalid_input(
                    ValidationViolation::Incompatible {
                        field: "message.continuation",
                        other: "message.role",
                    },
                    "Anthropic continuation requires an assistant message",
                ));
            }
            if continuation.model_id.as_str() != model {
                return Err(KitError::invalid_input(
                    ValidationViolation::Incompatible {
                        field: "message.continuation.model",
                        other: "selected_model",
                    },
                    "Anthropic continuation belongs to a different model",
                ));
            }
            return match &continuation.inner {
                NativeContinuation::Anthropic { content_blocks } => Ok(json!({
                    "role": "assistant",
                    "content": content_blocks,
                })),
                _ => Err(KitError::invalid_input(
                    ValidationViolation::Incompatible {
                        field: "message.continuation",
                        other: "selected_provider",
                    },
                    "Anthropic generation requires Anthropic continuation state",
                )),
            };
        }

        let render = match self {
            Self::User => user_content_part_to_block,
            Self::Assistant => assistant_content_part_to_block,
        };
        let mut content = message
            .content()
            .iter()
            .map(render)
            .collect::<Result<Vec<_>, _>>()?;
        let role = match self {
            Self::User => "user",
            Self::Assistant => {
                for tool_call in message.function_calls() {
                    let id = tool_call.id().ok_or_else(|| {
                        KitError::invalid_input(
                            ValidationViolation::MissingField {
                                field: "function_call.id",
                            },
                            "Anthropic assistant tool_use history requires a provider call id",
                        )
                    })?;
                    content.push(json!({
                        "type": "tool_use",
                        "id": id,
                        "name": tool_call.name(),
                        "input": tool_call.arguments(),
                    }));
                }
                "assistant"
            }
        };
        Ok(json!({"role": role, "content": content}))
    }
}

pub(crate) fn build_message_body(
    model: &str,
    thinking_profile: ClaudeThinkingProfile,
    request: GenerateRequestView<'_>,
    stream: bool,
) -> Result<Value, KitError> {
    let generation = request.generation;
    if generation.temperature().is_some() && generation.top_p().is_some() {
        return Err(KitError::invalid_input(
            ValidationViolation::Incompatible {
                field: "generation.temperature",
                other: "generation.top_p",
            },
            "Anthropic generation cannot combine temperature and top_p",
        ));
    }
    if generation.presence_penalty().is_some()
        || generation.frequency_penalty().is_some()
        || generation.seed().is_some()
    {
        return Err(KitError::unsupported(
            ValidationViolation::Unsupported {
                field: "generation",
                rule: "penalties and seed unavailable",
            },
            "Anthropic generation penalties or seed",
        ));
    }
    thinking_profile.validate_sampling(generation)?;

    let mut body = Map::new();
    body.insert("model".to_string(), Value::String(model.to_string()));
    let filtered_tools = filter_tools_for_choice(request.tools, generation.tool_choice())?;
    crate::providers::anthropic::tools::AnthropicHostedPlanner::new().validate(
        model,
        filtered_tools
            .iter()
            .filter_map(crate::generation::ProviderToolSpec::hosted_tool),
    )?;
    let tools = filtered_tools
        .iter()
        .map(tool_spec_to_value)
        .collect::<Result<Vec<_>, _>>()?;
    let max_tokens = generation
        .max_output_tokens()
        .map_or(DEFAULT_MAX_OUTPUT_TOKENS, std::num::NonZeroU32::get);
    body.insert("max_tokens".to_string(), json!(max_tokens));

    if let Some(system) = build_system_blocks(request)? {
        body.insert("system".to_string(), system);
    }
    body.insert(
        "messages".to_string(),
        Value::Array(build_messages_array(model, request)?),
    );
    if !tools.is_empty() {
        body.insert("tools".to_string(), Value::Array(tools));
    }
    if let Some(output_config) = response_format_to_output_config(request.response_format)? {
        body.insert("output_config".to_string(), output_config);
    }

    let thinking = match request.reasoning {
        Some(reasoning) => {
            if let Some(tokens) = reasoning.max_reasoning_tokens
                && tokens >= max_tokens
            {
                return Err(KitError::invalid_input(
                    ValidationViolation::OutOfRange {
                        field: "reasoning.max_reasoning_tokens",
                        rule: "less than generation.max_output_tokens",
                    },
                    "Anthropic reasoning max_reasoning_tokens must be less than generation.max_output_tokens",
                ));
            }
            thinking_profile.thinking(reasoning)?
        }
        None => None,
    };
    thinking_profile.validate_tool_choice(
        generation.tool_choice(),
        request
            .reasoning
            .is_some_and(|reasoning| reasoning.max_reasoning_tokens.is_some()),
    )?;
    if let Some(tool_choice) = generation.tool_choice() {
        body.insert("tool_choice".to_string(), tool_choice_to_value(tool_choice));
    }
    if let Some(reasoning) = request.reasoning {
        if let Some(thinking) = thinking {
            body.insert("thinking".to_string(), thinking);
        }
        if let Some(effort) = thinking_profile.effort(reasoning.effort)? {
            let output_config = body
                .entry("output_config".to_string())
                .or_insert_with(|| Value::Object(Map::new()))
                .as_object_mut()
                .ok_or_else(|| {
                    KitError::invalid_state("Anthropic output_config was not an object")
                })?;
            output_config.insert("effort".to_string(), Value::String(effort.to_string()));
        }
    }

    if let Some(temperature) = generation.temperature() {
        body.insert("temperature".to_string(), json!(temperature));
    }
    if let Some(top_p) = generation.top_p() {
        body.insert("top_p".to_string(), json!(top_p));
    }
    if let Some(top_k) = generation.top_k() {
        body.insert("top_k".to_string(), json!(top_k.get()));
    }
    if !generation.stop_sequences().is_empty() {
        body.insert(
            "stop_sequences".to_string(),
            json!(generation.stop_sequences()),
        );
    }
    if stream {
        body.insert("stream".to_string(), Value::Bool(true));
    }
    Ok(Value::Object(body))
}

fn tool_result_message_to_value(
    message: &Message<GenerationContinuation>,
) -> Result<Value, KitError> {
    if !message.content().is_empty() {
        return Err(KitError::invalid_input(
            ValidationViolation::InvalidValue {
                field: "message.content",
                rule: "tool content belongs in typed function results",
            },
            "Anthropic tool messages carry content through typed function results",
        ));
    }
    Ok(json!({
        "role": "user",
        "content": message.function_results()
            .iter()
            .map(|result| {
                let call_id = result.call_id().ok_or_else(|| {
                    KitError::invalid_input(
                        ValidationViolation::MissingField { field: "function_result.call_id" }, "Anthropic function results require a provider call id",
                    )
                })?;
                Ok(json!({
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": tool_result_content_value(result.content())?,
                    "is_error": result.is_error(),
                }))
            })
            .collect::<Result<Vec<_>, KitError>>()?,
    }))
}

fn tool_result_content_value(content: &[ContentPart]) -> Result<Value, KitError> {
    if let [ContentPart::Text { text }] = content {
        return Ok(Value::String(text.clone()));
    }
    if let [ContentPart::Json { value }] = content {
        return Ok(Value::String(value.to_string()));
    }
    content
        .iter()
        .map(tool_result_content_part_to_block)
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn build_system_blocks(request: GenerateRequestView<'_>) -> Result<Option<Value>, KitError> {
    let mut blocks = Vec::new();
    for message in request.messages {
        if !matches!(message.role(), Role::System | Role::Developer) {
            break;
        }
        for part in message.content() {
            let ContentPart::Text { text } = part else {
                return Err(KitError::invalid_input(
                    ValidationViolation::InvalidValue {
                        field: "message.content",
                        rule: "instructions require text-only content",
                    },
                    "Anthropic instructions require text-only content",
                ));
            };
            blocks.push(json!({"type": "text", "text": text}));
        }
    }
    Ok((!blocks.is_empty()).then_some(Value::Array(blocks)))
}

fn build_messages_array(
    model: &str,
    request: GenerateRequestView<'_>,
) -> Result<Vec<Value>, KitError> {
    let mut messages = Vec::new();
    for message in request.messages {
        match message.role() {
            Role::System | Role::Developer => {}
            Role::User => messages.push(AnthropicMessageRole::User.into_value(model, message)?),
            Role::Assistant => {
                messages.push(AnthropicMessageRole::Assistant.into_value(model, message)?)
            }
            Role::Tool => messages.push(tool_result_message_to_value(message)?),
        }
    }
    Ok(messages)
}

fn response_format_to_output_config(
    format: Option<&ResponseFormat>,
) -> Result<Option<Value>, KitError> {
    match format {
        None | Some(ResponseFormat::Text) => Ok(None),
        Some(ResponseFormat::JsonSchema(schema)) => Ok(Some(json!({
            "format": {
                "type": "json_schema",
                "schema": schema.schema(),
            },
        }))),
        Some(ResponseFormat::JsonObject) => Err(KitError::unsupported(
            ValidationViolation::Unsupported {
                field: "response_format",
                rule: "JSON object output requires a schema",
            },
            "Anthropic schema-free JSON output",
        )),
    }
}
