use serde_json::Value;

use crate::KitError;
use crate::error::ValidationViolation;
use crate::generation::{ReasoningConfig, ReasoningEffort, ReasoningSummaryMode};

#[derive(Clone, Copy, Debug)]
pub(crate) enum OllamaThinkingProfile {
    GptOssLevel,
    Boolean,
    Unavailable,
}

impl OllamaThinkingProfile {
    pub(crate) fn render_think_field(
        self,
        model_id: &str,
        reasoning: Option<&ReasoningConfig>,
    ) -> Result<Option<Value>, KitError> {
        let Some(reasoning) = reasoning else {
            return Ok(None);
        };

        if matches!(self, Self::Unavailable) {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning",
                    rule: "thinking unavailable for selected model",
                },
                format!("thinking by selected Ollama model `{model_id}`"),
            ));
        }

        if reasoning.max_reasoning_tokens.is_some() {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning.max_reasoning_tokens",
                    rule: "reasoning budget unavailable",
                },
                format!(
                    "Ollama model `{model_id}` does not expose a portable reasoning token budget"
                ),
            ));
        }

        match self {
            Self::GptOssLevel => {
                if reasoning.summary_mode != ReasoningSummaryMode::Auto {
                    return Err(KitError::unsupported(
                        ValidationViolation::Unsupported {
                            field: "reasoning.summary_mode",
                            rule: "summary mode unavailable",
                        },
                        format!("Ollama GPT-OSS model `{model_id}` reasoning summary mode"),
                    ));
                }
                let Some(effort) = reasoning.effort else {
                    return Err(KitError::invalid_input(
                        ValidationViolation::MissingField {
                            field: "reasoning.effort",
                        },
                        format!(
                            "Ollama GPT-OSS model `{model_id}` requires an explicit reasoning effort of low, medium, or high"
                        ),
                    ));
                };
                Ok(Some(Value::String(
                    self.render_effort(model_id, effort)?.to_string(),
                )))
            }
            Self::Boolean => {
                if reasoning.effort.is_some() {
                    return Err(KitError::unsupported(
                        ValidationViolation::Unsupported {
                            field: "reasoning.effort",
                            rule: "selected model uses boolean thinking",
                        },
                        format!(
                            "Ollama model `{model_id}` uses boolean think; reasoning effort levels are only valid for GPT-OSS"
                        ),
                    ));
                }
                if reasoning.summary_mode != ReasoningSummaryMode::Auto {
                    return Err(KitError::unsupported(
                        ValidationViolation::Unsupported {
                            field: "reasoning.summary_mode",
                            rule: "summary mode unavailable",
                        },
                        format!("Ollama model `{model_id}` reasoning summary mode"),
                    ));
                }
                Ok(Some(Value::Bool(true)))
            }
            Self::Unavailable => Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning",
                    rule: "thinking unavailable for selected model",
                },
                format!("thinking by selected Ollama model `{model_id}`"),
            )),
        }
    }

    fn render_effort(
        self,
        model_id: &str,
        effort: ReasoningEffort,
    ) -> Result<&'static str, KitError> {
        if !matches!(self, Self::GptOssLevel) {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning.effort",
                    rule: "leveled effort unavailable",
                },
                format!("Ollama model `{model_id}` does not accept leveled thinking effort"),
            ));
        }

        match effort {
            ReasoningEffort::Low => Ok("low"),
            ReasoningEffort::Medium => Ok("medium"),
            ReasoningEffort::High => Ok("high"),
            ReasoningEffort::Minimal => Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning.effort",
                    rule: "minimal effort unavailable",
                },
                format!("Ollama model `{model_id}` does not expose a portable minimal effort tier"),
            )),
            ReasoningEffort::Maximum => Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning.effort",
                    rule: "maximum effort unavailable",
                },
                format!("Ollama model `{model_id}` does not expose a portable maximum effort tier"),
            )),
        }
    }
}
