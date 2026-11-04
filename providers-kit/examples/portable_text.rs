//! Requires a running Ollama service with the exact selected model installed.

use providers_kit::config::{OllamaConfig, ProviderKind, TransportConfig};
use providers_kit::generation::{GenerateRequest, Message};
use providers_kit::{ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig};

#[tokio::main]
async fn main() -> Result<(), providers_kit::KitError> {
    let provider = Provider::from_config(ProviderConfig::new(
        ProviderKind::Ollama(OllamaConfig::local()),
        TransportConfig::default(),
    ))?;
    let work = async {
        let model = provider.model(ModelId::new("ministral-3:8b-instruct-2512-q4_K_M")?)?;
        let request = GenerateRequest::new(vec![Message::user_text(
            "Explain the architecture goals of providers-kit.",
        )?])?;
        model.generate(&request).await
    }
    .await;
    let response = match (work, provider.shutdown().await) {
        (Ok(response), Ok(())) => Ok(response),
        (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
    }?;
    println!("{:#?}", response.message);
    Ok(())
}
