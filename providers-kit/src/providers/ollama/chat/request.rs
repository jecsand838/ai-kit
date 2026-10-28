use base64::Engine as _;
use reqwest::Method;
use serde_json::{Map, Value};

use crate::KitError;
use crate::core::{GenerateRequestView, filter_tools_for_choice, text_parts};
use crate::error::ValidationViolation;
use crate::generation::{AllowedToolMode, ContentPart, NativeContinuation, Role, ToolChoice};
use crate::models::ModelId;
use crate::transport::HttpResponse;

use super::super::models::OllamaModel;
use super::super::{
    api::{OLLAMA_REQUESTS, OllamaApiContext},
    native::{
        structured_outputs::response_format_to_ollama, thinking::OllamaThinkingProfile,
        tool_calling::tool_spec_to_ollama,
    },
};
use super::types::OllamaBuiltChatBody;

pub(crate) async fn create_raw_chat(
    api: &OllamaApiContext,
    body: Value,
) -> Result<HttpResponse<Value>, KitError> {
    OLLAMA_REQUESTS
        .json::<Value>(
            "chat",
            api.request(Method::POST, "chat")?.json(&body),
            api.transport().execution(),
        )
        .await
}

pub(crate) fn build_chat_body(
    model: &ModelId,
    thinking_profile: OllamaThinkingProfile,
    request: GenerateRequestView<'_>,
    stream: bool,
) -> Result<OllamaBuiltChatBody, KitError> {
    let selected_tools = match request.generation.tool_choice() {
        Some(ToolChoice::None) => Vec::new(),
        choice => filter_tools_for_choice(request.tools, choice)?,
    };
    if matches!(
        request.generation.tool_choice(),
        Some(
            ToolChoice::Required
                | ToolChoice::Any
                | ToolChoice::Tool { .. }
                | ToolChoice::Allowed {
                    mode: AllowedToolMode::Required,
                    ..
                }
        )
    ) {
        return Err(KitError::unsupported(
            ValidationViolation::Unsupported {
                field: "tool_choice",
                rule: "forced selection unavailable",
            },
            "Ollama's native /api/chat endpoint does not support forced tool selection",
        ));
    }
    let history = build_history(model, request)?;
    let mut body = Map::new();
    body.insert(
        "model".to_string(),
        Value::String(model.as_str().to_string()),
    );
    body.insert("messages".to_string(), Value::Array(history));
    body.insert("stream".to_string(), Value::Bool(stream));

    let tools = selected_tools
        .iter()
        .map(tool_spec_to_ollama)
        .collect::<Result<Vec<_>, _>>()?;
    if !tools.is_empty() {
        body.insert("tools".to_string(), Value::Array(tools));
    }

    if let Some(format) = response_format_to_ollama(request.response_format)? {
        body.insert("format".to_string(), format);
    }

    if let Some(think) = thinking_profile.render_think_field(model.as_str(), request.reasoning)? {
        body.insert("think".to_string(), think);
    }

    let options = OllamaModel::chat_options(request)?;
    if !options.is_empty() {
        body.insert("options".to_string(), Value::Object(options));
    }

    Ok(OllamaBuiltChatBody {
        body: Value::Object(body),
        model_id: model.clone(),
    })
}

fn build_history(
    model_id: &ModelId,
    request: GenerateRequestView<'_>,
) -> Result<Vec<Value>, KitError> {
    let mut history = Vec::new();
    if let Some(system) = build_system_message(request)? {
        history.push(system);
    }
    for message in request.messages {
        if matches!(message.role(), Role::System | Role::Developer) {
            continue;
        }
        if !message.function_results().is_empty() {
            for result in message.function_results() {
                history.push(tool_result_to_wire(result)?);
            }
            continue;
        }
        history.push(message_to_wire(model_id, message)?);
    }
    Ok(history)
}

fn build_system_message(request: GenerateRequestView<'_>) -> Result<Option<Value>, KitError> {
    let mut parts = Vec::new();
    for message in request.messages {
        if matches!(message.role(), Role::System | Role::Developer) {
            let rendered = text_parts(message.content(), "\n\n")?;
            if !rendered.trim().is_empty() {
                parts.push(rendered);
            }
        }
    }
    let content = parts.join("\n\n");
    if content.trim().is_empty() {
        Ok(None)
    } else {
        Ok(Some(serde_json::json!({
            "role": "system",
            "content": content,
        })))
    }
}

fn message_to_wire(
    model_id: &ModelId,
    message: &crate::generation::Message,
) -> Result<Value, KitError> {
    let mut object = Map::new();
    object.insert(
        "role".to_string(),
        Value::String(
            match message.role() {
                Role::System | Role::Developer => "system",
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::Tool => "tool",
            }
            .to_string(),
        ),
    );

    let mut text = Vec::new();
    let mut images = Vec::new();
    let mut visible_thinking = None;
    for part in message.content() {
        match part {
            ContentPart::Text { text: value } => text.push(value.as_str()),
            ContentPart::Image { media_type, data }
                if message.role() == Role::User && media_type.starts_with("image/") =>
            {
                images.push(base64::engine::general_purpose::STANDARD.encode(data));
            }
            ContentPart::Thinking { text } if message.role() == Role::Assistant => {
                visible_thinking = text.as_deref();
            }
            other => {
                return Err(KitError::unsupported(
                    ValidationViolation::Unsupported {
                        field: "message.content",
                        rule: "unsupported input modality for selected model",
                    },
                    format!("Ollama {:?} message content {other:?}", message.role()),
                ));
            }
        }
    }
    object.insert("content".to_string(), Value::String(text.join("\n")));
    if !images.is_empty() {
        object.insert(
            "images".to_string(),
            Value::Array(images.into_iter().map(Value::String).collect()),
        );
    }

    match message.continuation() {
        Some(continuation) if continuation.model_id.as_str() != model_id.as_str() => {
            return Err(KitError::invalid_input(
                ValidationViolation::Incompatible {
                    field: "message.continuation.model",
                    other: "selected_model",
                },
                "Ollama continuation belongs to a different model",
            ));
        }
        Some(continuation) => {
            let NativeContinuation::Ollama { thinking } = &continuation.inner else {
                return Err(KitError::invalid_input(
                    ValidationViolation::Incompatible {
                        field: "message.continuation",
                        other: "selected_provider",
                    },
                    "Ollama cannot replay continuation state from another provider",
                ));
            };
            if visible_thinking.is_some_and(|visible| visible != thinking) {
                return Err(KitError::invalid_input(
                    ValidationViolation::Incompatible {
                        field: "message.thinking",
                        other: "message.continuation",
                    },
                    "Ollama visible thinking did not match continuation state",
                ));
            }
            if !thinking.is_empty() {
                object.insert("thinking".to_string(), Value::String(thinking.clone()));
            }
        }
        None if visible_thinking.is_some() => {
            return Err(KitError::invalid_input(
                ValidationViolation::MissingField {
                    field: "message.continuation",
                },
                "Ollama thinking replay requires continuation state",
            ));
        }
        None => {}
    }

    if !message.function_calls().is_empty() {
        object.insert(
            "tool_calls".to_string(),
            Value::Array(
                message
                    .function_calls()
                    .iter()
                    .map(super::super::native::tool_calling::tool_call_to_ollama)
                    .collect(),
            ),
        );
    }

    Ok(Value::Object(object))
}

fn tool_result_to_wire(result: &crate::generation::FunctionResult) -> Result<Value, KitError> {
    let mut value = serde_json::json!({
        "role": "tool",
        "tool_name": result.name(),
        "content": crate::core::tool_result_text(result.content())?,
    });
    if let Some(id) = result.call_id() {
        value["tool_call_id"] = Value::String(id.to_owned());
    }
    Ok(value)
}
