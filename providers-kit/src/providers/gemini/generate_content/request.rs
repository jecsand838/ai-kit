use serde_json::{Map, Value};

use crate::KitError;
use crate::core::{GenerateRequestView, HostedToolMode};
use crate::models::ModelId;
use crate::transport::HttpResponse;

use super::super::{
    api::{GEMINI_REQUESTS, GeminiApiContext},
    native::service_tier::render_service_tier,
    reasoning::GeminiThinkingProfile,
};
use super::{
    config::build_generation_config,
    content::{build_contents, build_system_instruction},
    tools::build_tools,
};

pub(crate) async fn create_raw_generate_content(
    api: &GeminiApiContext,
    model: &str,
    body: Value,
) -> Result<HttpResponse<Value>, KitError> {
    let path = format!("v1beta/{}:generateContent", model);
    GEMINI_REQUESTS
        .json::<Value>(
            &path,
            api.request(reqwest::Method::POST, &path)?.json(&body),
            api.transport().execution(),
        )
        .await
}

pub(crate) fn build_generate_body(
    api: &GeminiApiContext,
    model: &ModelId,
    thinking_profile: GeminiThinkingProfile,
    request: GenerateRequestView<'_>,
    mode: HostedToolMode,
) -> Result<Value, KitError> {
    let mut root = Map::new();
    root.insert(
        "contents".to_string(),
        Value::Array(build_contents(model, request)?),
    );

    if let Some(system_instruction) = build_system_instruction(request)? {
        root.insert("systemInstruction".to_string(), system_instruction);
    }

    let generation_config = build_generation_config(thinking_profile, request)?;
    if !generation_config.is_empty() {
        root.insert(
            "generationConfig".to_string(),
            Value::Object(generation_config),
        );
    }

    let (tools, tool_config) = build_tools(model, request, mode)?;
    if !tools.is_empty() {
        root.insert("tools".to_string(), Value::Array(tools));
    }
    if let Some(tool_config) = tool_config {
        root.insert("toolConfig".to_string(), tool_config);
    }

    if let Some(service_tier) = render_service_tier(api.config().service_tier()) {
        root.insert("serviceTier".to_string(), Value::String(service_tier));
    }

    Ok(Value::Object(root))
}
