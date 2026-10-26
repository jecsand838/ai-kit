use provider_sdk::config::{OllamaConfig, ProviderKind, TransportConfig};
use provider_sdk::generation::{GenerateRequest, Message};
use provider_sdk::{ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig};

#[tokio::main]
async fn main() -> Result<(), provider_sdk::SdkError> {
    let provider = Provider::from_config(ProviderConfig::new(
        ProviderKind::Ollama(OllamaConfig::local()),
        TransportConfig::default(),
    ))?;
    let model = provider.model(ModelId::new("ministral-3:8b-instruct-2512-q4_K_M")?)?;
    let request = GenerateRequest::new(vec![Message::user_text(
        "Explain the architecture goals of provider-sdk.",
    )?])?;

    let response = model.generate(&request).await?;
    println!("{:#?}", response.message);
    Ok(())
}
