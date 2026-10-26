use serde_json::{Map, Value};

use crate::SdkError;
use crate::core::GenerateRequestView;
use crate::generation::ResponseFormat;

use super::super::reasoning::GeminiThinkingProfile;

pub(super) fn build_generation_config(
    thinking_profile: GeminiThinkingProfile,
    request: GenerateRequestView<'_>,
) -> Result<Map<String, Value>, SdkError> {
    thinking_profile.validate_sampling(request.generation)?;
    let mut generation = Map::new();
    if let Some(value) = request.generation.temperature() {
        generation.insert("temperature".to_string(), Value::from(value));
    }
    if let Some(value) = request.generation.top_p() {
        generation.insert("topP".to_string(), Value::from(value));
    }
    if let Some(value) = request.generation.top_k() {
        generation.insert("topK".to_string(), Value::from(value.get()));
    }
    if let Some(value) = request.generation.max_output_tokens() {
        generation.insert("maxOutputTokens".to_string(), Value::from(value.get()));
    }
    if !request.generation.stop_sequences().is_empty() {
        generation.insert(
            "stopSequences".to_string(),
            Value::Array(
                request
                    .generation
                    .stop_sequences()
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    if let Some(value) = request.generation.presence_penalty() {
        generation.insert("presencePenalty".to_string(), Value::from(value));
    }
    if let Some(value) = request.generation.frequency_penalty() {
        generation.insert("frequencyPenalty".to_string(), Value::from(value));
    }
    if let Some(value) = request.generation.seed() {
        generation.insert("seed".to_string(), Value::from(value));
    }

    if let Some(response_format) = request.response_format {
        match response_format {
            ResponseFormat::JsonObject => {
                generation.insert(
                    "responseMimeType".to_string(),
                    Value::String("application/json".to_string()),
                );
            }
            ResponseFormat::JsonSchema(schema) => {
                generation.insert(
                    "responseMimeType".to_string(),
                    Value::String("application/json".to_string()),
                );
                generation.insert("responseJsonSchema".to_string(), schema.schema().clone());
            }
            ResponseFormat::Text => {}
        }
    }

    if let Some(reasoning) = request.reasoning
        && let Some(thinking) = thinking_profile.config(reasoning)?
    {
        generation.insert("thinkingConfig".to_string(), Value::Object(thinking));
    }

    Ok(generation)
}
