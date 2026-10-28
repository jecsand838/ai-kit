use serde_json::{Value, json};

use crate::KitError;
use crate::error::ValidationViolation;
use crate::generation::{
    AllowedToolMode, GenerationConfig, ReasoningConfig, ReasoningEffort, ReasoningSummaryMode,
    ToolChoice,
};

// Exact thinking modes, not model-name prefix inference. See the provider note
// for current model, effort, sampling, and forced-tool authorities.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ClaudeThinkingProfile {
    Adaptive { forced_tools: bool },
    AdaptiveWithLegacyBudget,
    Extended,
    Unknown,
}

impl ClaudeThinkingProfile {
    pub(crate) fn for_model(model: &str) -> Self {
        match model {
            "claude-fable-5-1" | "claude-opus-5-5" | "claude-sonnet-5-5" => Self::Adaptive {
                forced_tools: false,
            },
            "claude-haiku-5-5" | "claude-fable-5" | "claude-opus-5" | "claude-sonnet-5"
            | "claude-opus-4-8" | "claude-opus-4-7" => Self::Adaptive { forced_tools: true },
            "claude-opus-4-6" | "claude-sonnet-4-6" => Self::AdaptiveWithLegacyBudget,
            "claude-haiku-4-5" | "claude-haiku-4-5-20251001" => Self::Extended,
            _ => Self::Unknown,
        }
    }

    pub(super) fn validate_sampling(self, generation: &GenerationConfig) -> Result<(), KitError> {
        if matches!(self, Self::Adaptive { .. })
            && (generation.temperature().is_some()
                || generation.top_p().is_some()
                || generation.top_k().is_some())
        {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "generation",
                    rule: "selected model has fixed sampling",
                },
                "Anthropic sampling overrides for this adaptive-only model",
            ));
        }
        Ok(())
    }

    pub(super) fn thinking(self, reasoning: &ReasoningConfig) -> Result<Option<Value>, KitError> {
        if matches!(self, Self::Unknown) {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning",
                    rule: "selected model lacks recognized reasoning controls",
                },
                "Anthropic reasoning controls for an unrecognized model ID",
            ));
        }
        let mut thinking = if let Some(tokens) = reasoning.max_reasoning_tokens {
            if matches!(self, Self::Adaptive { .. }) {
                return Err(KitError::unsupported(
                    ValidationViolation::Unsupported {
                        field: "reasoning.max_reasoning_tokens",
                        rule: "manual budget unavailable for selected model",
                    },
                    "Anthropic manual reasoning budgets for this adaptive-thinking model",
                ));
            }
            if tokens < 1_024 {
                return Err(KitError::invalid_input(
                    ValidationViolation::OutOfRange {
                        field: "reasoning.max_reasoning_tokens",
                        rule: "at least 1024 tokens",
                    },
                    "Anthropic manual reasoning requires at least 1024 tokens",
                ));
            }
            json!({"type": "enabled", "budget_tokens": tokens})
        } else if reasoning.effort.is_some()
            || !matches!(reasoning.summary_mode, ReasoningSummaryMode::Auto)
        {
            if matches!(self, Self::Extended) {
                return Err(KitError::unsupported(
                    ValidationViolation::Unsupported {
                        field: "reasoning",
                        rule: "selected model requires a manual budget",
                    },
                    "Anthropic adaptive thinking for a manual-budget model",
                ));
            }
            json!({"type": "adaptive"})
        } else {
            return Ok(None);
        };
        match reasoning.summary_mode {
            ReasoningSummaryMode::Auto => {}
            ReasoningSummaryMode::Hidden => thinking["display"] = json!("omitted"),
            ReasoningSummaryMode::Summary | ReasoningSummaryMode::Verbose => {
                thinking["display"] = json!("summarized");
            }
        }
        Ok(Some(thinking))
    }

    pub(super) fn effort(
        self,
        effort: Option<ReasoningEffort>,
    ) -> Result<Option<&'static str>, KitError> {
        if effort.is_some() && matches!(self, Self::Extended | Self::Unknown) {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning.effort",
                    rule: "adaptive effort unavailable for selected model",
                },
                "Anthropic adaptive reasoning effort for selected model",
            ));
        }
        Ok(effort.map(|effort| match effort {
            ReasoningEffort::Minimal | ReasoningEffort::Low => "low",
            ReasoningEffort::Medium => "medium",
            ReasoningEffort::High => "high",
            ReasoningEffort::Maximum => "max",
        }))
    }

    pub(super) fn validate_tool_choice(
        self,
        choice: Option<&ToolChoice>,
        manual_thinking: bool,
    ) -> Result<(), KitError> {
        let forced = matches!(
            choice,
            Some(
                ToolChoice::Required
                    | ToolChoice::Any
                    | ToolChoice::Tool { .. }
                    | ToolChoice::Allowed {
                        mode: AllowedToolMode::Required,
                        ..
                    }
            )
        );
        if forced
            && matches!(
                self,
                Self::Adaptive {
                    forced_tools: false
                }
            )
        {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "generation.tool_choice",
                    rule: "forced tool use unavailable for selected model",
                },
                "Anthropic forced tool use for selected model",
            ));
        }
        if forced && manual_thinking {
            return Err(KitError::invalid_input(
                ValidationViolation::Incompatible {
                    field: "reasoning",
                    other: "tool_choice",
                },
                "Anthropic manual thinking only supports tool_choice auto or none",
            ));
        }
        Ok(())
    }
}
