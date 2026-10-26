use serde_json::{Map, Value};

use crate::SdkError;
use crate::core::GenerateRequestView;

// Low-level Ollama option renderer for selected chat models. Unsupported
// sampling controls return honest errors from the generation path; do not
// silently ignore, synthesize, or route them through metadata/native options.
// Source: https://docs.ollama.com/api/chat
impl super::super::models::OllamaModel {
    pub(in crate::providers::ollama) fn chat_options(
        request: GenerateRequestView<'_>,
    ) -> Result<Map<String, Value>, SdkError> {
        let mut options = Map::new();
        if let Some(value) = request.generation.temperature() {
            options.insert("temperature".to_string(), Value::from(value));
        }
        if let Some(value) = request.generation.top_p() {
            options.insert("top_p".to_string(), Value::from(value));
        }
        if let Some(value) = request.generation.top_k() {
            options.insert("top_k".to_string(), Value::from(value.get()));
        }
        if let Some(value) = request.generation.seed() {
            options.insert("seed".to_string(), Value::from(value));
        }
        if !request.generation.stop_sequences().is_empty() {
            options.insert(
                "stop".to_string(),
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
        if let Some(value) = request.generation.max_output_tokens() {
            options.insert("num_predict".to_string(), Value::from(value.get()));
        }
        if let Some(value) = request.generation.presence_penalty() {
            options.insert("presence_penalty".to_owned(), Value::from(value));
        }
        if let Some(value) = request.generation.frequency_penalty() {
            options.insert("frequency_penalty".to_owned(), Value::from(value));
        }
        Ok(options)
    }
}
