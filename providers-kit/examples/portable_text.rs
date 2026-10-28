use providers_kit::config::{OllamaConfig, ProviderKind, TransportConfig};
use providers_kit::generation::{GenerateRequest, Message};
use providers_kit::{ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig};

#[tokio::main]
async fn main() -> Result<(), providers_kit::KitError> {
    let provider = Provider::from_config(ProviderConfig::new(
        ProviderKind::Ollama(OllamaConfig::local()),
        TransportConfig::default(),
    ))?;
    let model = provider.model(ModelId::new("ministral-3:8b-instruct-2512-q4_K_M")?)?;
    let request = GenerateRequest::new(vec![Message::user_text(
        "Explain the architecture goals of providers-kit.",
    )?])?;

    let response = model.generate(&request).await?;
    println!("{:#?}", response.message);
    Ok(())
}
