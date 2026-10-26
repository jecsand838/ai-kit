use serde_json::{Map, Value, json};

use crate::SdkError;
use crate::error::ValidationViolation;
use crate::generation::{GenerationConfig, ReasoningConfig, ReasoningEffort, ReasoningSummaryMode};

#[derive(Clone, Copy, Debug)]
pub(crate) enum GeminiThinkingProfile {
    Levels { minimal: bool, fixed_sampling: bool },
    Budget,
    Unknown,
}

impl GeminiThinkingProfile {
    pub(crate) fn for_model(model: &str) -> Self {
        match model {
            "models/gemini-4-argon" | "models/gemini-3.8-flash" => Self::Levels {
                minimal: false,
                fixed_sampling: true,
            },
            "models/gemini-3.1-pro-preview" => Self::Levels {
                minimal: false,
                fixed_sampling: false,
            },
            "models/gemini-3-flash-preview" => Self::Levels {
                minimal: true,
                fixed_sampling: false,
            },
            "models/gemini-2.5-pro"
            | "models/gemini-2.5-flash"
            | "models/gemini-2.5-flash-lite" => Self::Budget,
            _ => Self::Unknown,
        }
    }

    pub(crate) fn validate_sampling(self, generation: &GenerationConfig) -> Result<(), SdkError> {
        if matches!(
            self,
            Self::Levels {
                fixed_sampling: true,
                ..
            }
        ) && (generation.temperature().is_some()
            || generation.top_p().is_some()
            || generation.top_k().is_some())
        {
            return Err(SdkError::unsupported(
                ValidationViolation::Unsupported {
                    field: "generation",
                    rule: "selected model ignores sampling overrides",
                },
                "Gemini sampling overrides for the selected model",
            ));
        }
        Ok(())
    }

    pub(crate) fn config(
        self,
        reasoning: &ReasoningConfig,
    ) -> Result<Option<Map<String, Value>>, SdkError> {
        let mut thinking = Map::new();
        match self {
            Self::Levels { minimal, .. } => {
                if reasoning.max_reasoning_tokens.is_some() {
                    return Err(SdkError::unsupported(
                        ValidationViolation::Unsupported {
                            field: "reasoning.max_reasoning_tokens",
                            rule: "selected model uses effort levels",
                        },
                        "Gemini selected model uses thinkingLevel rather than a token budget",
                    ));
                }
                if let Some(effort) = reasoning.effort {
                    let level = match effort {
                        ReasoningEffort::Minimal if minimal => "minimal",
                        ReasoningEffort::Minimal | ReasoningEffort::Low => "low",
                        ReasoningEffort::Medium => "medium",
                        ReasoningEffort::High | ReasoningEffort::Maximum => "high",
                    };
                    thinking.insert("thinkingLevel".into(), json!(level));
                }
            }
            Self::Budget => {
                if reasoning.effort.is_some() {
                    return Err(SdkError::unsupported(
                        ValidationViolation::Unsupported {
                            field: "reasoning.effort",
                            rule: "selected model uses token budgets",
                        },
                        "Gemini 2.5 models use thinkingBudget rather than effort levels",
                    ));
                }
                if let Some(tokens) = reasoning.max_reasoning_tokens {
                    thinking.insert("thinkingBudget".into(), json!(tokens));
                }
            }
            Self::Unknown => {
                return Err(SdkError::unsupported(
                    ValidationViolation::Unsupported {
                        field: "reasoning",
                        rule: "selected model lacks recognized reasoning controls",
                    },
                    "Gemini reasoning controls for an unrecognized model ID",
                ));
            }
        }
        match reasoning.summary_mode {
            ReasoningSummaryMode::Hidden => {
                thinking.insert("includeThoughts".into(), Value::Bool(false));
            }
            ReasoningSummaryMode::Summary | ReasoningSummaryMode::Verbose => {
                thinking.insert("includeThoughts".into(), Value::Bool(true));
            }
            ReasoningSummaryMode::Auto => {}
        }
        Ok((!thinking.is_empty()).then_some(thinking))
    }
}
