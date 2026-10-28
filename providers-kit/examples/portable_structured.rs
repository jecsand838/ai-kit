use providers_kit::config::{OllamaConfig, ProviderKind, TransportConfig};
use providers_kit::generation::{GenerateRequest, JsonSchema, Message, ResponseFormat};
use providers_kit::{ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), providers_kit::KitError> {
    let provider = Provider::from_config(ProviderConfig::new(
        ProviderKind::Ollama(OllamaConfig::local()),
        TransportConfig::default(),
    ))?;
    let model = provider.model(ModelId::new("ministral-3:8b-instruct-2512-q4_K_M")?)?;
    let request = GenerateRequest::new(vec![Message::user_text(
        "Return a JSON object with keys `name` and `summary`.",
    )?])?
    .with_response_format(ResponseFormat::JsonSchema(JsonSchema::new(json!({
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "summary": {"type": "string"}
        },
        "required": ["name", "summary"],
        "additionalProperties": false
    }))?));
    let response = model.generate(&request).await?;

    println!("{:#?}", response.message);
    Ok(())
}
