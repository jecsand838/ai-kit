use serde_json::{Map, Value};

use crate::KitError;
use crate::core::GenerateRequestView;
use crate::error::ValidationViolation;
use crate::generation::{ReasoningEffort, ReasoningSummaryMode};
use crate::models::ModelId;

// Lower-level OpenAI reasoning renderer for selected GPT models. It emits the
// Responses reasoning object after model dispatch has selected a generation
// model; unsupported model and operation combinations return `Unsupported`.
// Source: https://developers.openai.com/api/docs/models
impl super::super::models::OpenAiModel {
    pub(in crate::providers::openai) fn insert_response_reasoning_fields(
        body: &mut Map<String, Value>,
        model: &ModelId,
        request: GenerateRequestView<'_>,
    ) -> Result<(), KitError> {
        let Some(reasoning) = request.reasoning else {
            return Ok(());
        };
        if reasoning.max_reasoning_tokens.is_some() {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "reasoning.max_reasoning_tokens",
                    rule: "explicit reasoning budget unavailable",
                },
                "OpenAI explicit maximum reasoning-token control",
            ));
        }

        let mut reasoning_object = Map::new();
        if let Some(effort) = reasoning.effort {
            reasoning_object.insert(
                "effort".to_string(),
                Value::String(
                    match effort {
                        ReasoningEffort::Minimal
                            if matches!(model.as_str(), "gpt-6-astra" | "gpt-6.1-sol") =>
                        {
                            "low"
                        }
                        ReasoningEffort::Minimal => "none",
                        ReasoningEffort::Low => "low",
                        ReasoningEffort::Medium => "medium",
                        ReasoningEffort::High => "high",
                        ReasoningEffort::Maximum
                            if matches!(
                                model.as_str(),
                                "gpt-6-astra"
                                    | "gpt-6.1-sol"
                                    | "gpt-6-sol"
                                    | "gpt-6-luna"
                                    | "gpt-5.6"
                                    | "gpt-5.6-sol"
                                    | "gpt-5.6-terra"
                                    | "gpt-5.6-luna"
                                    | "gpt-5.6-cyber"
                            ) =>
                        {
                            "max"
                        }
                        ReasoningEffort::Maximum => "xhigh",
                    }
                    .to_string(),
                ),
            );
        }

        let summary_value = match reasoning.summary_mode {
            ReasoningSummaryMode::Auto => Some("auto"),
            ReasoningSummaryMode::Hidden => None,
            ReasoningSummaryMode::Summary => Some("concise"),
            ReasoningSummaryMode::Verbose => Some("detailed"),
        };
        if let Some(summary) = summary_value {
            reasoning_object.insert("summary".to_string(), Value::String(summary.to_string()));
        }

        if !reasoning_object.is_empty() {
            body.insert("reasoning".to_string(), Value::Object(reasoning_object));
        }
        Ok(())
    }
}
