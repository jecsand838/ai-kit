use crate::backends::codex::conversations::thread::CodexThreadBinding;
use crate::backends::codex::state_store::records::CodexNativeSelection;
use crate::model::PersonalitySelector;
use crate::{
    KitError, ModelSelector, PromptRequest, ReasoningEffort, StructuredOutputRequest, ToolSelection,
};

const MAX_CODEX_OUTPUT_SCHEMA_BYTES: usize = 64 << 10;

#[derive(Clone, Debug)]
pub(crate) struct CodexPrompt {
    thread_id: String,
    client_message_id: String,
    text: String,
    cwd: std::path::PathBuf,
    selection: CodexPromptSelection,
    structured_output: Option<CodexStructuredOutput>,
    timeout: Option<std::time::Duration>,
}

#[derive(Clone, Debug)]
pub(crate) struct CodexPromptSelection {
    model: Option<ModelSelector>,
    reasoning_effort: Option<ReasoningEffort>,
    previous: CodexNativeSelection,
    accepted: CodexNativeSelection,
}

#[derive(Clone, Debug)]
pub(crate) struct CodexStructuredOutput {
    schema: serde_json::Value,
    validator: jsonschema::Validator,
}

impl CodexPrompt {
    pub(crate) fn validate_request(request: &PromptRequest) -> Result<(), KitError> {
        CodexPromptSelection::validate_personality(request.personality())?;
        if request.instructions().is_some() {
            return Err(KitError::unsupported(
                "cannot represent prompt-level instructions",
            ));
        }
        if request
            .tool_selection()
            .is_some_and(|selection| !matches!(selection, ToolSelection::Auto))
        {
            return Err(KitError::unsupported(
                "can represent only absent or automatic portable tool selection",
            ));
        }
        Ok(())
    }

    pub(crate) fn new(
        binding: &CodexThreadBinding,
        text: String,
        cwd: std::path::PathBuf,
        selection: CodexPromptSelection,
        structured_output: Option<CodexStructuredOutput>,
        timeout: Option<std::time::Duration>,
    ) -> Self {
        Self {
            thread_id: binding.thread_id().to_owned(),
            client_message_id: format!("msg_{}", uuid::Uuid::new_v4().simple()),
            text,
            cwd,
            selection,
            structured_output,
            timeout,
        }
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn client_message_id(&self) -> &str {
        &self.client_message_id
    }

    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    pub(crate) fn cwd(&self) -> &std::path::Path {
        &self.cwd
    }

    pub(crate) fn model(&self) -> Option<&ModelSelector> {
        self.selection.model()
    }

    pub(crate) fn reasoning_effort(&self) -> Option<&ReasoningEffort> {
        self.selection.reasoning_effort()
    }

    pub(crate) fn selection(&self) -> &CodexPromptSelection {
        &self.selection
    }

    pub(crate) fn output_schema(&self) -> Option<&serde_json::Value> {
        self.structured_output
            .as_ref()
            .map(CodexStructuredOutput::schema)
    }

    pub(crate) fn structured_output(&self) -> Option<&CodexStructuredOutput> {
        self.structured_output.as_ref()
    }

    pub(crate) fn timeout(&self) -> Option<std::time::Duration> {
        self.timeout
    }
}

impl CodexPromptSelection {
    pub(crate) fn validate_personality(
        personality: Option<&PersonalitySelector>,
    ) -> Result<(), KitError> {
        if personality.is_some() {
            return Err(KitError::unsupported(
                "personality selection is not supported",
            ));
        }
        Ok(())
    }

    pub(crate) fn new(
        model: Option<ModelSelector>,
        reasoning_effort: Option<ReasoningEffort>,
        previous: CodexNativeSelection,
        accepted: CodexNativeSelection,
    ) -> Self {
        Self {
            model,
            reasoning_effort,
            previous,
            accepted,
        }
    }

    pub(crate) fn model(&self) -> Option<&ModelSelector> {
        self.model.as_ref()
    }

    pub(crate) fn reasoning_effort(&self) -> Option<&ReasoningEffort> {
        self.reasoning_effort.as_ref()
    }

    pub(crate) fn previous(&self) -> &CodexNativeSelection {
        &self.previous
    }

    pub(crate) fn accepted(&self) -> &CodexNativeSelection {
        &self.accepted
    }
}

impl CodexStructuredOutput {
    pub(crate) fn compile(request: &StructuredOutputRequest) -> Result<Self, KitError> {
        Self::from_schema(request.schema().clone())
    }

    pub(crate) fn from_schema(schema: serde_json::Value) -> Result<Self, KitError> {
        if schema.as_object().is_none_or(serde_json::Map::is_empty) {
            return Err(KitError::invalid_input(
                "structured output schema must be a nonempty JSON object",
            ));
        }
        let encoded = serde_json::to_vec(&schema)
            .map_err(|_| KitError::invalid_input("structured output schema is not encodable"))?;
        if encoded.len() > MAX_CODEX_OUTPUT_SCHEMA_BYTES {
            return Err(KitError::invalid_input(
                "structured output schema exceeds the persistence bound",
            ));
        }
        let validator = jsonschema::draft202012::new(&schema).map_err(|_| {
            KitError::invalid_input("structured output schema could not be compiled")
        })?;
        Ok(Self { schema, validator })
    }

    pub(crate) fn schema(&self) -> &serde_json::Value {
        &self.schema
    }

    pub(crate) fn parse(&self, text: &str) -> Result<serde_json::Value, KitError> {
        let value = serde_json::from_str(text)
            .map_err(|_| KitError::protocol("structured output is not valid JSON"))?;
        self.validator
            .validate(&value)
            .map_err(|_| KitError::protocol("structured output does not satisfy its schema"))?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CodexTurnStart {
    turn_id: String,
}

impl CodexTurnStart {
    pub(crate) fn decode(value: serde_json::Value) -> Result<Self, KitError> {
        let turn = value
            .get("turn")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| KitError::protocol("prompt acceptance response is malformed"))?;
        let turn_id = turn
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|turn_id| !turn_id.is_empty() && turn_id.trim() == *turn_id)
            .ok_or_else(|| {
                KitError::protocol("prompt acceptance response omitted its execution identity")
            })?;
        if turn.get("status").and_then(serde_json::Value::as_str) != Some("inProgress")
            || !turn.get("items").is_some_and(serde_json::Value::is_array)
            || turn.get("error").is_some_and(|error| !error.is_null())
            || turn
                .get("completedAt")
                .is_some_and(|completed| !completed.is_null())
            || turn
                .get("durationMs")
                .is_some_and(|duration| !duration.is_null())
            || turn
                .get("startedAt")
                .is_some_and(|started| !started.is_null() && started.as_i64().is_none())
            || turn.get("itemsView").is_some_and(|view| {
                !matches!(view.as_str(), Some("notLoaded" | "summary" | "full"))
            })
        {
            return Err(KitError::protocol(
                "prompt acceptance response contains contradictory execution state",
            ));
        }
        Ok(Self {
            turn_id: turn_id.to_owned(),
        })
    }

    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }
}
