use crate::live_failures::CaseFailure;
use providers_kit::error::{ErrorKind, Operation};

use providers_kit::Model;
use providers_kit::generation::{
    ContentPart, FinishReason, FunctionResult, GenerateRequest, GenerationConfig, JsonSchema,
    Message, ProviderFunctionTool, ProviderToolSpec, ReasoningConfig, ReasoningEffort, ToolChoice,
};
use providers_kit::models::ModelBackend;

use super::{LiveProvider, Mode, Service, StreamCompletion};
use crate::live_assertions::{ErrorEvidence, GenerationEvidence};

impl LiveProvider {
    async fn exact_generation(&self, mode: Mode) -> Result<(), CaseFailure> {
        let nonce = Self::nonce();
        let instruction = match (self.service(), mode) {
            (Service::Ollama, Mode::Streaming) => format!(
                "Copy the following token character for character, including its complete suffix: {nonce}. Treat it as literal text; do not infer, repair, shorten, or replace any character. Return only the token, without quotes, Markdown, or whitespace."
            ),
            _ => format!("Reply with exactly this text and nothing else: {nonce}"),
        };
        let request = Self::text_request(&instruction)?;
        let response = self.generate(&request, mode).await?;
        GenerationEvidence::exact_text(&response, &nonce, false).map_err(CaseFailure::from)
    }

    async fn reasoning_generation(&self) -> Result<(), CaseFailure> {
        let (variable, reasoning) = match self.service() {
            Service::Anthropic => (
                "TEST_ANTHROPIC_MODEL_THINKING",
                ReasoningConfig {
                    effort: Some(ReasoningEffort::High),
                    ..ReasoningConfig::default()
                },
            ),
            Service::Gemini => (
                "TEST_GEMINI_MODEL_THINKING_LEVEL",
                ReasoningConfig {
                    effort: Some(ReasoningEffort::High),
                    ..ReasoningConfig::default()
                },
            ),
            Service::Ollama => (
                "TEST_OLLAMA_MODEL_REASONING",
                ReasoningConfig {
                    effort: Some(ReasoningEffort::High),
                    ..ReasoningConfig::default()
                },
            ),
            Service::OpenAi => {
                return Err("no separate OpenAI reasoning fixture configured"
                    .to_owned()
                    .into());
            }
        };
        let model = self.selected(&Self::required(variable)?)?;
        let nonce = Self::nonce();
        let request = Self::text_request(&format!(
            "Reply with exactly this text and nothing else: {nonce}"
        ))?
        .with_reasoning(reasoning);
        let response = self.generate_on(&model, &request, Mode::Unary).await?;
        GenerationEvidence::exact_text(&response, &nonce, false).map_err(CaseFailure::from)
    }

    async fn missing_model(&self, mode: Mode) -> Result<(), CaseFailure> {
        let absent = match self.service() {
            Service::Gemini => format!("models/{}", Self::nonce()),
            Service::OpenAi | Service::Anthropic | Service::Ollama => Self::nonce(),
        };
        let model = self.selected(&absent)?;
        let request = GenerateRequest::new(vec![
            Message::user_text("Reply with exactly: unreachable").map_err(Self::error)?,
        ])
        .map_err(Self::error)?;
        let error = self.rejected(&model, &request, mode).await?;
        let codes: &[Option<&str>] = match self.service() {
            Service::OpenAi => &[Some("model_not_found")],
            Service::Anthropic => &[Some("not_found_error")],
            Service::Gemini => &[Some("NOT_FOUND"), Some("404")],
            Service::Ollama => &[None],
        };
        let operation = match mode {
            Mode::Unary => Operation::Generate,
            Mode::Streaming => Operation::GenerateStream,
        };
        ErrorEvidence::not_found(&error, operation, &absent, codes)
            .map_err(|failure| CaseFailure::from(failure).with_sdk(error))
    }

    pub(super) fn function_tool() -> Result<ProviderToolSpec, CaseFailure> {
        let schema = JsonSchema::new(serde_json::json!({
            "type": "object", "properties": {"value": {"type": "string"}},
            "required": ["value"], "additionalProperties": false,
        }))
        .map_err(Self::error)?;
        Ok(ProviderToolSpec::Function(
            ProviderFunctionTool::new(
                "strict_lookup",
                Some("Returns the requested verification token.".to_owned()),
                schema,
            )
            .map_err(Self::error)?,
        ))
    }

    async fn function_continuation(&self) -> Result<(), CaseFailure> {
        let model = self.model()?;
        self.function_continuation_on(&model, Mode::Unary, None, Self::generation_config(), None)
            .await
    }

    pub(super) async fn function_continuation_on(
        &self,
        model: &Model,
        mode: Mode,
        reasoning: Option<ReasoningConfig>,
        generation: GenerationConfig,
        system_instruction: Option<&str>,
    ) -> Result<(), CaseFailure> {
        let user = Message::user_text("Call strict_lookup exactly once with value `providers-kit`. After it returns, copy the verification_token from its result character for character as an opaque literal string, including its complete prefix and suffix. Do not interpret, regenerate, shorten, correct, or replace any character. Return only the complete token, with no quotes, Markdown, whitespace, or surrounding text.")
            .map_err(Self::error)?;
        let tool = Self::function_tool()?;
        let choice = if self.service() == Service::Ollama
            || (self.service() == Service::Anthropic
                && (reasoning.is_some()
                    || matches!(
                        model.model_id().as_str(),
                        "claude-fable-5-1" | "claude-opus-5-5" | "claude-sonnet-5-5"
                    ))) {
            ToolChoice::Auto
        } else {
            ToolChoice::Tool {
                name: "strict_lookup".to_owned(),
            }
        };
        // Freeze scenario inputs before either request; only the initial call selects a tool.
        let initial_generation = generation.clone().with_tool_choice(choice);
        let prefix = match system_instruction {
            Some(instruction) => vec![
                Message::system(vec![ContentPart::Text {
                    text: instruction.to_owned(),
                }])
                .map_err(Self::error)?,
                user,
            ],
            None => vec![user],
        };
        if self.service() == Service::Ollama
            && matches!(mode, Mode::Unary)
            && reasoning.is_none()
            && std::env::var_os("PROVIDERS_KIT_EVIDENCE_DIR").is_some()
        {
            Self::write_required_observation(
                "function-continuation-scenario",
                serde_json::json!({
                    "stage": "before-initial-request",
                    "model": model.model_id().as_str(),
                    "prefix": &prefix,
                    "initial_generation": &initial_generation,
                    "continuation_generation": &generation,
                }),
            )?;
        }
        let mut request = GenerateRequest::new(prefix.clone())
            .map_err(Self::error)?
            .with_tools(vec![tool.clone()])
            .map_err(Self::error)?
            .with_generation(initial_generation)
            .map_err(Self::error)?;
        if let Some(reasoning) = &reasoning {
            request = request.with_reasoning(reasoning.clone());
        }
        let first = self
            .generate_with_completion(model, &request, mode, StreamCompletion::RequireFunctionCall)
            .await?;
        if first.finish_reason != Some(FinishReason::ToolCall)
            || first.message.function_calls().len() != 1
        {
            return Err(format!(
                "expected exactly one function call and ToolCall finish, got {:?}",
                first.finish_reason
            )
            .into());
        }
        let call = &first.message.function_calls()[0];
        if call.name() != "strict_lookup"
            || call.arguments() != &serde_json::json!({"value": "providers-kit"})
        {
            return Err("function name or exact arguments did not match request"
                .to_owned()
                .into());
        }
        let nonce = Self::nonce();
        let result = FunctionResult::new(
            call.id().map(str::to_owned),
            call.name().to_owned(),
            vec![ContentPart::Json {
                value: serde_json::json!({"verification_token": nonce}),
            }],
            false,
        )
        .map_err(Self::error)?;
        if self.service() == Service::Ollama
            && matches!(mode, Mode::Unary)
            && reasoning.is_none()
            && std::env::var_os("PROVIDERS_KIT_EVIDENCE_DIR").is_some()
        {
            Self::write_required_observation(
                "function-continuation-input",
                serde_json::json!({
                    "stage": "before-continuation",
                    "model": model.model_id().as_str(),
                    "expected_nonce": &nonce,
                    "function_result": &result,
                }),
            )?;
        }
        // Move the complete returned assistant message without rebuilding its continuation.
        let mut messages = prefix;
        messages.extend([
            first.message,
            Message::tool(vec![result]).map_err(Self::error)?,
        ]);
        let mut continuation = GenerateRequest::new(messages)
            .map_err(Self::error)?
            .with_tools(vec![tool])
            .map_err(Self::error)?
            .with_generation(generation)
            .map_err(Self::error)?;
        if let Some(reasoning) = reasoning {
            continuation = continuation.with_reasoning(reasoning);
        }
        let response = self.generate_on(model, &continuation, mode).await?;
        GenerationEvidence::exact_text(&response, &nonce, false).map_err(CaseFailure::from)
    }

    async fn forced_function_unsupported(&self, mode: Mode) -> Result<(), CaseFailure> {
        let request = Self::text_request("Call strict_lookup with value providers-kit.")?
            .with_tools(vec![Self::function_tool()?])
            .map_err(Self::error)?
            .with_generation(
                GenerationConfig::default().with_tool_choice(ToolChoice::Tool {
                    name: "strict_lookup".to_owned(),
                }),
            )
            .map_err(Self::error)?;
        let model = self.model()?;
        let result = match mode {
            Mode::Unary => model.generate(&request).await.map(|_| ()),
            Mode::Streaming => model.generate_stream(&request).await.map(|_| ()),
        };
        match result {
            Err(error) if error.kind() == ErrorKind::Unsupported => Ok(()),
            Err(error) => Err(CaseFailure::from(
                "Ollama forced tool choice did not return Unsupported",
            )
            .with_sdk(error)),
            Ok(()) => Err("Ollama forced tool choice unexpectedly succeeded".into()),
        }
    }
}

#[tokio::test]
async fn openai_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.exact_generation(Mode::Unary).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.exact_generation(Mode::Unary).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.exact_generation(Mode::Unary).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.exact_generation(Mode::Unary).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn openai_streaming() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.exact_generation(Mode::Streaming).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_streaming() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.exact_generation(Mode::Streaming).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_streaming() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.exact_generation(Mode::Streaming).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_streaming() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.exact_generation(Mode::Streaming).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn openai_missing_model_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.missing_model(Mode::Unary).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_missing_model_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.missing_model(Mode::Unary).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_missing_model_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.missing_model(Mode::Unary).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_missing_model_generation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.missing_model(Mode::Unary).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn openai_missing_model_streaming() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.missing_model(Mode::Streaming).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_missing_model_streaming() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.missing_model(Mode::Streaming).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_missing_model_streaming() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.missing_model(Mode::Streaming).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_missing_model_streaming() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.missing_model(Mode::Streaming).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_reasoning() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.reasoning_generation().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_reasoning() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.reasoning_generation().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_reasoning() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.reasoning_generation().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn openai_function_continuation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.function_continuation().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_function_continuation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.function_continuation().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_function_continuation() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.function_continuation().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_function_continuation() -> Result<(), CaseFailure> {
    let generation = LiveProvider::generation_config();
    let system_instruction = Some(
        "You are a tool-result relay. After the requested tool call, return exactly the verification_token string supplied by the tool, preserving every character, and nothing else. Never invent a token.",
    );
    let live = LiveProvider::new(Service::Ollama)?;
    let result = async {
        let model = live.model()?;
        live.function_continuation_on(&model, Mode::Unary, None, generation, system_instruction)
            .await
    }
    .await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn ollama_forced_function_generation_unsupported() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.forced_function_unsupported(Mode::Unary).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_forced_function_streaming_unsupported() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.forced_function_unsupported(Mode::Streaming).await;
    live.finish(result).await;
    Ok(())
}
