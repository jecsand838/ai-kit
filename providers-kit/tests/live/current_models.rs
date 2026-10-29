use providers_kit::generation::{JsonSchema, ReasoningConfig, ReasoningEffort, ResponseFormat};
use serde_json::json;

use super::{LiveProvider, Mode, Service};
use crate::live_assertions::GenerationEvidence;
use crate::live_failures::CaseFailure;

impl LiveProvider {
    async fn current_model_generation(&self, model_id: &str) -> Result<(), CaseFailure> {
        let model = self.selected(model_id)?;
        let nonce = Self::nonce();
        let mut request = Self::text_request(&format!(
            "Reply with exactly this text and nothing else: {nonce}"
        ))?
        .with_reasoning(ReasoningConfig {
            effort: Some(ReasoningEffort::Minimal),
            ..ReasoningConfig::default()
        });
        if model_id == "gpt-6-luna" {
            request = request.with_generation(
                Self::generation_config()
                    .with_temperature(0.2)?
                    .with_top_p(0.9)?,
            )?;
        }
        let response = self.generate_on(&model, &request, Mode::Unary).await?;
        GenerationEvidence::exact_text(&response, &nonce, false).map_err(CaseFailure::from)
    }

    async fn current_reasoning_boundaries(&self, model_id: &str) -> Result<(), CaseFailure> {
        let model = self.selected(model_id)?;
        for (mode, effort) in [
            (Mode::Unary, None),
            (Mode::Streaming, Some(ReasoningEffort::Maximum)),
        ] {
            let nonce = Self::nonce();
            let mut request = Self::text_request(&format!(
                "Reply with exactly this text and nothing else: {nonce}"
            ))?;
            if let Some(effort) = effort {
                request = request.with_reasoning(ReasoningConfig {
                    effort: Some(effort),
                    ..ReasoningConfig::default()
                });
            }
            let response = self.generate_on(&model, &request, mode).await?;
            GenerationEvidence::exact_text(&response, &nonce, false)?;
        }
        Ok(())
    }

    async fn current_structured_output(&self, model_id: &str) -> Result<(), CaseFailure> {
        let model = self.selected(model_id)?;
        let schema = JsonSchema::new(json!({
            "type":"object", "properties":{"ack":{"type":"string"}},
            "required":["ack"], "additionalProperties":false,
        }))?;
        for mode in [Mode::Unary, Mode::Streaming] {
            let nonce = Self::nonce();
            let request = Self::text_request(&format!("Return a JSON object with exactly one property, ack, whose string value is {nonce}."))?
                .with_response_format(ResponseFormat::JsonSchema(schema.clone()));
            let response = self.generate_on(&model, &request, mode).await?;
            GenerationEvidence::completed(&response)?;
            if !response.hosted_tool_outputs.is_empty() {
                return Err("structured generation returned unrequested hosted output".into());
            }
            let value: serde_json::Value =
                serde_json::from_str(&GenerationEvidence::text(&response)?).map_err(|_| {
                    CaseFailure::from("structured generation returned invalid JSON")
                })?;
            if value != json!({"ack": nonce}) {
                return Err(
                    "structured generation violated the exact object schema or requested value"
                        .into(),
                );
            }
        }
        Ok(())
    }

    async fn current_reasoning_continuation(&self, model_id: &str) -> Result<(), CaseFailure> {
        let model = self.selected(model_id)?;
        self.function_continuation_on(
            &model,
            Mode::Streaming,
            Some(ReasoningConfig {
                effort: Some(ReasoningEffort::High),
                ..ReasoningConfig::default()
            }),
            Self::generation_config(),
            None,
        )
        .await
    }
}

#[tokio::test]
async fn openai_current_astra_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.current_model_generation("gpt-6-astra").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn openai_current_sol_6_1_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.current_model_generation("gpt-6.1-sol").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn openai_current_luna_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.current_model_generation("gpt-6-luna").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn openai_retained_sol_6_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.current_model_generation("gpt-6-sol").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn anthropic_current_fable_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.current_model_generation("claude-fable-5-1").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn anthropic_current_opus_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.current_model_generation("claude-opus-5-5").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn anthropic_current_sonnet_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.current_model_generation("claude-sonnet-5-5").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn anthropic_current_haiku_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.current_model_generation("claude-haiku-5-5").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn gemini_current_argon_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.current_model_generation("models/gemini-4-argon").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn gemini_current_flash_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live
        .current_model_generation("models/gemini-3.8-flash")
        .await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn openai_current_astra_reasoning_boundaries() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.current_reasoning_boundaries("gpt-6-astra").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn openai_current_luna_reasoning_boundaries() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.current_reasoning_boundaries("gpt-6-luna").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn anthropic_current_reasoning_boundaries() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.current_reasoning_boundaries("claude-haiku-5-5").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn gemini_current_reasoning_boundaries() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live
        .current_reasoning_boundaries("models/gemini-3.8-flash")
        .await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn openai_current_structured_output() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.current_structured_output("gpt-6-luna").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn anthropic_current_structured_output() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.current_structured_output("claude-haiku-5-5").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn gemini_current_structured_output() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live
        .current_structured_output("models/gemini-3.8-flash")
        .await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn openai_current_reasoning_stream_continuation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.current_reasoning_continuation("gpt-6-luna").await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn anthropic_current_reasoning_stream_continuation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live
        .current_reasoning_continuation("claude-haiku-5-5")
        .await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn gemini_current_reasoning_stream_continuation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live
        .current_reasoning_continuation("models/gemini-3.8-flash")
        .await;
    live.finish(result).await;
    Ok(())
}
