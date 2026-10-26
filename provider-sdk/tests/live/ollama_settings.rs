use std::num::NonZeroU32;
use std::time::Duration;

use provider_sdk::ModelBackend;
use provider_sdk::embedding::EmbedRequest;
use provider_sdk::generation::{JsonSchema, ReasoningConfig, ResponseFormat};
use serde_json::json;

use super::{LiveProvider, Mode, Service};
use crate::live_assertions::GenerationEvidence;
use crate::live_failures::CaseFailure;

impl LiveProvider {
    async fn ollama_reduced_embeddings(&self) -> Result<(), CaseFailure> {
        let model = self.selected(&Self::required("TEST_OLLAMA_MODEL_EMBEDDINGS")?)?;
        let request = EmbedRequest::new(vec![
            "First document about an orchard.".into(),
            "Second document about a river.".into(),
        ])
        .map_err(Self::error)?
        .with_dimensions(NonZeroU32::new(512).expect("fixed nonzero dimensions"));
        let response = tokio::time::timeout(Duration::from_secs(180), model.embed(&request))
            .await
            .map_err(|_| CaseFailure::from("reduced embeddings exceeded 180 seconds"))?
            .map_err(Self::error)?;
        if response.vectors.len() != 2
            || response
                .vectors
                .iter()
                .any(|vector| vector.len() != 512 || vector.iter().any(|value| !value.is_finite()))
        {
            return Err("embedding count, dimensions, or finite values were incorrect".into());
        }
        if response.vectors[0] == response.vectors[1] {
            return Err("distinct embedding inputs returned identical vectors".into());
        }
        Ok(())
    }

    async fn ollama_penalties_and_usage(&self) -> Result<(), CaseFailure> {
        for mode in [Mode::Unary, Mode::Streaming] {
            let nonce = Self::nonce();
            let request =
                Self::text_request(&format!("Reply with exactly {nonce} and no other text."))?
                    .with_generation(
                        Self::generation_config()
                            .with_presence_penalty(0.2)
                            .map_err(Self::error)?
                            .with_frequency_penalty(0.3)
                            .map_err(Self::error)?,
                    )
                    .map_err(Self::error)?;
            let response = self.generate(&request, mode).await?;
            GenerationEvidence::exact_text(&response, &nonce, false).map_err(CaseFailure::from)?;
            // Absence and zero are valid observations; never generate again to obtain a hit.
            eprintln!(
                "Ollama cache observation mode={mode:?} cached_input_tokens={:?}",
                response.usage.cached_input_tokens
            );
        }
        Ok(())
    }

    async fn ollama_boolean_thinking(&self, exact: &str) -> Result<(), CaseFailure> {
        let model = self.selected(exact)?;
        let nonce = Self::nonce();
        let request =
            Self::text_request(&format!("Reply with exactly {nonce} and no other text."))?
                .with_reasoning(ReasoningConfig::default());
        let response = self.generate_on(&model, &request, Mode::Unary).await?;
        GenerationEvidence::exact_text(&response, &nonce, false).map_err(CaseFailure::from)
    }

    async fn ollama_local_schema(&self) -> Result<(), CaseFailure> {
        for mode in [Mode::Unary, Mode::Streaming] {
            let nonce = Self::nonce();
            let schema = JsonSchema::new(json!({"type":"object","properties":{"token":{"type":"string","const":nonce}},"required":["token"],"additionalProperties":false})).map_err(Self::error)?;
            let request = Self::text_request(&format!(
                "Return a JSON object with the sole property token equal to {nonce}."
            ))?
            .with_response_format(ResponseFormat::JsonSchema(schema));
            let response = self.generate(&request, mode).await?;
            GenerationEvidence::completed(&response).map_err(CaseFailure::from)?;
            let text = GenerationEvidence::text(&response).map_err(CaseFailure::from)?;
            let observed: serde_json::Value = serde_json::from_str(&text)
                .map_err(|_| CaseFailure::from("schema response was not JSON"))?;
            if observed != json!({"token":nonce}) {
                return Err("schema response did not preserve exact token and shape".into());
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn ollama_reduced_embedding_dimensions() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.ollama_reduced_embeddings().await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn ollama_sampling_penalties_and_reported_cache_usage() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.ollama_penalties_and_usage().await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn ollama_gemma_current_boolean_thinking() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.ollama_boolean_thinking("gemma4:e4b-it-qat").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn ollama_qwen_current_boolean_thinking() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.ollama_boolean_thinking("qwen3.5:9b-q4_K_M").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn ollama_local_structured_output() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.ollama_local_schema().await;
    live.finish(result).await;
    Ok(())
}
