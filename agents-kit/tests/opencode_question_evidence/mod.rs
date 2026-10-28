pub mod readback;

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub struct QuestionEvidence {
    question: NativeQuestion,
    root_user: String,
    prompt: String,
    model: NativeModel,
    configured_model: String,
}

pub struct QuestionExpectation<'a> {
    session: &'a str,
    prompt: &'a str,
    configured_model: &'a str,
}

#[derive(Debug, Serialize)]
pub struct QuestionGrade {
    provider_id: String,
    model_id: String,
    configured_model: String,
    model_exact_copy: bool,
    model_yes_description_exact_copy: bool,
    model_no_description_exact_copy: bool,
}

#[derive(Debug, Deserialize, PartialEq)]
struct NativeModel {
    #[serde(rename = "providerID")]
    provider_id: String,
    #[serde(rename = "modelID")]
    model_id: String,
}

#[derive(Deserialize)]
struct NativeProviders {
    all: Vec<NativeProvider>,
    connected: Vec<String>,
}

#[derive(Deserialize)]
struct NativeProvider {
    id: String,
    models: BTreeMap<String, NativeModelInfo>,
}

#[derive(Deserialize)]
struct NativeModelInfo {
    id: String,
    #[serde(rename = "providerID")]
    provider_id: String,
    name: String,
}

#[derive(Deserialize)]
struct NativeQuestion {
    id: String,
    #[serde(rename = "sessionID")]
    session_id: String,
    questions: Vec<QuestionInfo>,
    tool: ToolOrigin,
}

#[derive(Deserialize)]
struct QuestionInfo {
    header: String,
    question: String,
    options: Vec<QuestionOption>,
    multiple: bool,
    #[serde(default = "QuestionInfo::custom_default")]
    custom: bool,
}

#[derive(Deserialize)]
struct QuestionOption {
    label: String,
    description: String,
}

#[derive(Deserialize)]
struct ToolOrigin {
    #[serde(rename = "messageID")]
    message_id: String,
    #[serde(rename = "callID")]
    call_id: String,
}

#[derive(Deserialize)]
struct NativeMessage {
    info: MessageInfo,
    parts: Vec<MessagePart>,
}

#[derive(Deserialize)]
#[serde(tag = "role", rename_all = "lowercase")]
enum MessageInfo {
    User {
        id: String,
        #[serde(rename = "sessionID")]
        session_id: String,
        model: NativeModel,
        #[serde(flatten)]
        attributes: BTreeMap<String, Value>,
    },
    Assistant {
        id: String,
        #[serde(rename = "sessionID")]
        session_id: String,
        #[serde(rename = "parentID")]
        parent_id: String,
        #[serde(flatten)]
        model: NativeModel,
        time: AssistantTime,
        finish: Option<String>,
        #[serde(flatten)]
        attributes: BTreeMap<String, Value>,
    },
}

#[derive(Deserialize)]
struct AssistantTime {
    created: u64,
    completed: Option<u64>,
}

#[derive(Deserialize)]
struct MessagePart {
    id: String,
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "messageID")]
    message_id: String,
    #[serde(flatten)]
    content: PartContent,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum PartContent {
    Text {
        text: String,
        #[serde(default)]
        synthetic: bool,
    },
    Tool {
        #[serde(rename = "callID")]
        call_id: String,
        tool: String,
        state: ToolState,
    },
    Reasoning,
    StepStart,
    StepFinish,
    Snapshot,
    Patch,
    Agent,
    Retry,
    Compaction,
    File,
    Subtask,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum ToolState {
    Pending {
        input: Value,
    },
    Running {
        input: Value,
    },
    Completed {
        input: Value,
        output: String,
        metadata: ToolMetadata,
        time: ToolTime,
    },
    Error {
        input: Value,
    },
}

#[derive(Deserialize)]
struct ToolMetadata {
    answers: Vec<Vec<String>>,
    truncated: bool,
}

#[derive(Deserialize)]
struct ToolTime {
    start: u64,
    end: u64,
    compacted: Option<u64>,
}

impl QuestionInfo {
    fn tool_input(&self) -> Value {
        // The pending native descriptions are the fidelity oracle. Their exact
        // copying from the prompt is graded separately after the turn completes.
        serde_json::json!({ "questions": [{
            "header": self.header, "question": self.question, "multiple": self.multiple,
            "options": self.options.iter().map(|option| serde_json::json!({
                "label": option.label, "description": option.description,
            })).collect::<Vec<_>>(),
        }] })
    }

    fn mismatch_context(&self) -> String {
        // Only typed question fields enter this diagnostic. Six string previews
        // of at most 32 escaped scalars and two options keep the error under 4 KiB.
        let options: Vec<Value> = self
            .options
            .iter()
            .take(2)
            .map(|option| {
                serde_json::json!({
                    "label": Self::diagnostic_text(&option.label),
                    "description": Self::diagnostic_text(&option.description),
                })
            })
            .collect();
        let context = serde_json::json!({
            "expected": {
                "header": "Continue", "question": "Continue?", "multiple": false, "custom": true,
                "options": [
                    {"label": "Yes", "description": "Continue"},
                    {"label": "No", "description": "Stop"},
                ],
            },
            "observed": {
                "header": Self::diagnostic_text(&self.header),
                "question": Self::diagnostic_text(&self.question),
                "multiple": self.multiple,
                "custom": self.custom,
                "options": options,
                "option_count": self.options.len(),
                "options_truncated": self.options.len() > 2,
            },
        });
        format!(
            "native pending question differs from the original typed question; typed_question={context}"
        )
    }

    fn diagnostic_text(text: &str) -> Value {
        let mut characters = text.chars();
        let preview: String = characters
            .by_ref()
            .take(32)
            .flat_map(char::escape_default)
            .collect();
        serde_json::json!({
            "preview": preview,
            "bytes": text.len(),
            "truncated": characters.next().is_some(),
        })
    }

    // Selected Question.Info explicitly defaults omitted custom to true.
    fn custom_default() -> bool {
        true
    }
}

impl<'a> QuestionExpectation<'a> {
    pub fn new(session: &'a str, prompt: &'a str, configured_model: &'a str) -> Self {
        Self {
            session,
            prompt,
            configured_model,
        }
    }
}

impl QuestionEvidence {
    pub fn begin(
        pending: &[u8],
        tool_message: &[u8],
        user_message: &[u8],
        providers: &[u8],
        expected: QuestionExpectation<'_>,
    ) -> Result<Self, String> {
        let mut questions: Vec<NativeQuestion> = Self::decode(pending)?;
        if questions.len() != 1 {
            return Err(
                "native question inventory must contain exactly one pending request".into(),
            );
        }
        let question = questions.remove(0);
        if !Self::identity(&question.id, "que")
            || question.session_id != expected.session
            || question.tool.call_id.is_empty()
            || question.tool.call_id.trim() != question.tool.call_id
            || question.questions.len() != 1
        {
            return Err("native pending question has wrong ownership or cardinality".into());
        }
        let info = &question.questions[0];
        if info.header != "Continue"
            || info.question != "Continue?"
            || info.multiple
            || !info.custom
            || info.options.len() != 2
            || info.options[0].label != "Yes"
            || info.options[1].label != "No"
        {
            return Err(info.mismatch_context());
        }
        let inventory: NativeProviders = Self::decode(providers)?;
        let mut matches = Vec::new();
        let mut provider_ids = BTreeSet::new();
        for provider in inventory.all {
            if !provider_ids.insert(provider.id.clone()) {
                return Err("native provider inventory repeats an identity".into());
            }
            if !inventory.connected.contains(&provider.id) {
                continue;
            }
            for (key, model) in provider.models {
                if key != model.id || model.provider_id != provider.id {
                    return Err("native model inventory contradicts its parent/key".into());
                }
                if model.name == expected.configured_model {
                    matches.push(NativeModel {
                        provider_id: provider.id.clone(),
                        model_id: model.id,
                    });
                }
            }
        }
        if matches.len() != 1 {
            return Err("configured native model must have exactly one connected match".into());
        }
        let model = matches.remove(0);
        let assistant: NativeMessage = Self::decode(tool_message)?;
        assistant.validate_parts()?;
        let MessageInfo::Assistant {
            id,
            session_id,
            parent_id,
            model: actual,
            attributes,
            ..
        } = &assistant.info
        else {
            return Err("native question origin must be an Assistant".into());
        };
        if id != &question.tool.message_id
            || session_id != expected.session
            || actual != &model
            || attributes.contains_key("error")
        {
            return Err("native question origin has wrong message/session/model".into());
        }
        if !Self::identity(parent_id, "msg") || parent_id >= id {
            return Err("native Question does not causally follow its root User".into());
        }
        let root_user = parent_id.clone();
        let mut tools = assistant
            .parts
            .iter()
            .filter(|part| matches!(part.content, PartContent::Tool { .. }));
        let part = tools
            .next()
            .ok_or("native question origin lacks its tool part")?;
        if tools.next().is_some() {
            return Err("native question origin has additional tool calls".into());
        }
        let PartContent::Tool {
            call_id,
            tool,
            state,
        } = &part.content
        else {
            unreachable!()
        };
        let expected_input = info.tool_input();
        if call_id != &question.tool.call_id
            || tool != "question"
            || state.input() != &expected_input
        {
            return Err(state.mismatch_context(
                call_id == &question.tool.call_id,
                tool == "question",
                &expected_input,
            ));
        }
        if !matches!(state, ToolState::Running { .. }) {
            return Err("native pending question must have a running tool owner".into());
        }
        let user: NativeMessage = Self::decode(user_message)?;
        user.validate_parts()?;
        if !matches!(&user.info, MessageInfo::User { id, session_id, model: actual, attributes }
            if id == &root_user && session_id == expected.session && actual == &model && !attributes.contains_key("parentID"))
            || user.text() != expected.prompt
        {
            return Err("native root User does not match the original prompt/session/model".into());
        }
        Ok(Self {
            question,
            root_user,
            prompt: expected.prompt.to_owned(),
            model,
            configured_model: expected.configured_model.to_owned(),
        })
    }

    pub fn assess(
        self,
        messages: &[u8],
        answer: &str,
        portable: &str,
    ) -> Result<QuestionGrade, String> {
        if answer.is_empty() || matches!(answer, "Yes" | "No") || self.prompt.contains(answer) {
            return Err(
                "answer must be fresh custom text outside the original prompt/options".into(),
            );
        }
        let messages: Vec<NativeMessage> = Self::decode(messages)?;
        let mut ids = BTreeSet::new();
        let mut part_ids = BTreeSet::new();
        let mut tools = Vec::new();
        let mut terminal = Vec::new();
        let mut users = 0;
        for message in &messages {
            message.validate_parts()?;
            if !ids.insert(message.info.id()) || message.info.session() != self.question.session_id
            {
                return Err("completed native inventory has duplicate/foreign messages".into());
            }
            for part in &message.parts {
                if !part_ids.insert(&part.id) {
                    return Err("completed native inventory repeats a part identity".into());
                }
            }
            match &message.info {
                MessageInfo::User {
                    id,
                    model,
                    attributes,
                    ..
                } => {
                    if id != &self.root_user
                        || model != &self.model
                        || attributes.contains_key("parentID")
                        || message.text() != self.prompt
                    {
                        return Err(
                            "completed native inventory contains a stale/foreign User".into()
                        );
                    }
                    users += 1;
                }
                MessageInfo::Assistant {
                    parent_id,
                    model,
                    time,
                    attributes,
                    finish,
                    ..
                } => {
                    if parent_id != &self.root_user
                        || model != &self.model
                        || attributes.contains_key("error")
                        || time
                            .completed
                            .is_none_or(|completed| completed < time.created)
                    {
                        return Err(
                            "completed native Assistant lacks exact successful root ownership"
                                .into(),
                        );
                    }
                    if finish.as_deref() == Some("stop") {
                        terminal.push(message);
                    } else if finish.as_deref() != Some("tool-calls") {
                        return Err("completed native Assistant has no successful finish".into());
                    }
                    for part in &message.parts {
                        if let PartContent::Tool {
                            call_id,
                            tool,
                            state,
                        } = &part.content
                        {
                            tools.push((message.info.id(), call_id, tool, state));
                        }
                    }
                }
            }
        }
        if users != 1 || tools.len() != 1 || terminal.len() != 1 {
            return Err(
                "completed native inventory needs one User, one Question call, and one terminal"
                    .into(),
            );
        }
        let (message_id, call_id, tool, state) = tools[0];
        if message_id != self.question.tool.message_id
            || call_id != &self.question.tool.call_id
            || tool != "question"
        {
            return Err("completed native tool differs from the pending call".into());
        }
        let ToolState::Completed {
            input,
            output,
            metadata,
            time,
        } = state
        else {
            return Err("native Question did not complete successfully".into());
        };
        if input != &self.question.questions[0].tool_input()
            || metadata.answers != vec![vec![answer.to_owned()]]
            || metadata.truncated
            || time.end < time.start
            || time.compacted.is_some()
            || output
                != &format!(
                    "User has answered your questions: \"Continue?\"=\"{answer}\". You can now continue with the user's answers in mind."
                )
        {
            return Err(
                "completed native Question does not preserve the exact fresh answer/input/result"
                    .into(),
            );
        }
        if terminal[0].info.id() <= self.question.tool.message_id.as_str()
            || terminal[0].text() != portable
        {
            return Err(
                "portable terminal differs from the correlated native terminal text".into(),
            );
        }
        Ok(QuestionGrade {
            provider_id: self.model.provider_id,
            model_id: self.model.model_id,
            configured_model: self.configured_model,
            model_exact_copy: portable == answer,
            model_yes_description_exact_copy: self.question.questions[0].options[0].description
                == "Continue",
            model_no_description_exact_copy: self.question.questions[0].options[1].description
                == "Stop",
        })
    }

    pub fn require_no_pending(pending: &[u8]) -> Result<(), String> {
        let questions: Vec<NativeQuestion> = Self::decode(pending)?;
        if questions.is_empty() {
            Ok(())
        } else {
            Err("native request remains pending after resolution".into())
        }
    }

    pub fn pending_origin(pending: &[u8]) -> Result<(String, String), String> {
        let questions: Vec<NativeQuestion> = Self::decode(pending)?;
        if questions.len() != 1 {
            return Err("native pending inventory is missing or ambiguous".into());
        }
        Ok((
            questions[0].session_id.clone(),
            questions[0].tool.message_id.clone(),
        ))
    }

    pub fn parent_user(message: &[u8]) -> Result<String, String> {
        let message: NativeMessage = Self::decode(message)?;
        match message.info {
            MessageInfo::Assistant { parent_id, .. } => Ok(parent_id),
            MessageInfo::User { .. } => Err("question tool owner is not an Assistant".into()),
        }
    }

    fn identity(value: &str, prefix: &str) -> bool {
        value.starts_with(prefix)
            && value.len() > prefix.len()
            && !value.chars().any(char::is_whitespace)
    }

    fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
        serde_json::from_slice(bytes)
            .map_err(|error| format!("native evidence shape is invalid: {error}"))
    }
}

impl QuestionGrade {
    pub fn model_exact_copy(&self) -> bool {
        self.model_exact_copy
    }

    pub fn emit(&self, case: &str, runtime_version: &str) {
        println!(
            "\nAGENTS_KIT_QUESTION_GRADE={}",
            serde_json::json!({
                "version": 2, "case": case, "sdk_contract": true,
                "model_exact_copy": self.model_exact_copy(), "runtime_version": runtime_version,
                "model_yes_description_exact_copy": self.model_yes_description_exact_copy,
                "model_no_description_exact_copy": self.model_no_description_exact_copy,
                "provider_id": self.provider_id, "model_id": self.model_id, "configured_model": self.configured_model,
            })
        );
    }
}

impl MessageInfo {
    fn id(&self) -> &str {
        match self {
            Self::User { id, .. } | Self::Assistant { id, .. } => id,
        }
    }
    fn session(&self) -> &str {
        match self {
            Self::User { session_id, .. } | Self::Assistant { session_id, .. } => session_id,
        }
    }
}

impl NativeMessage {
    fn validate_parts(&self) -> Result<(), String> {
        let mut ids = BTreeSet::new();
        if !QuestionEvidence::identity(self.info.id(), "msg")
            || !QuestionEvidence::identity(self.info.session(), "ses")
        {
            return Err("native message has invalid branded identity".into());
        }
        for part in &self.parts {
            if !QuestionEvidence::identity(&part.id, "prt")
                || !ids.insert(&part.id)
                || part.message_id != self.info.id()
                || part.session_id != self.info.session()
            {
                return Err("native part has duplicate or foreign ownership".into());
            }
        }
        Ok(())
    }

    fn text(&self) -> String {
        let mut parts: Vec<_> = self
            .parts
            .iter()
            .filter_map(|part| match &part.content {
                PartContent::Text { text, synthetic } if !*synthetic => {
                    Some((&part.id, text.as_str()))
                }
                _ => None,
            })
            .collect();
        parts.sort_by_key(|(id, _)| *id);
        parts.into_iter().map(|(_, text)| text).collect()
    }
}

impl ToolState {
    fn mismatch_context(
        &self,
        call_id_matches: bool,
        tool_name_matches: bool,
        expected: &Value,
    ) -> String {
        let input = self.input();
        // Inspect only fixed paths. Unknown keys and values never enter the log.
        let fields: BTreeMap<&str, Value> = [
            ("root", ""),
            ("questions", "/questions"),
            ("entry", "/questions/0"),
            ("options", "/questions/0/options"),
            ("option0", "/questions/0/options/0"),
            ("option1", "/questions/0/options/1"),
            ("header", "/questions/0/header"),
            ("question", "/questions/0/question"),
            ("label0", "/questions/0/options/0/label"),
            ("description0", "/questions/0/options/0/description"),
            ("label1", "/questions/0/options/1/label"),
            ("description1", "/questions/0/options/1/description"),
            ("multiple", "/questions/0/multiple"),
            ("custom", "/questions/0/custom"),
        ]
        .into_iter()
        .map(|(name, path)| {
            (
                name,
                Self::diagnostic_input(input.pointer(path), expected.pointer(path)),
            )
        })
        .collect();
        let state = match self {
            Self::Pending { .. } => "pending",
            Self::Running { .. } => "running",
            Self::Completed { .. } => "completed",
            Self::Error { .. } => "error",
        };
        let context = serde_json::json!({
            "call_id_matches": call_id_matches,
            "tool_name_matches": tool_name_matches,
            "input_matches": input == expected,
            "state": state,
            "fields": fields,
        });
        format!("native question tool identity or arguments differ; tool_input={context}")
    }

    fn diagnostic_input(actual: Option<&Value>, expected: Option<&Value>) -> Value {
        let mut context = match actual {
            None => serde_json::json!({"kind": "missing"}),
            Some(Value::Null) => serde_json::json!({"kind": "null"}),
            Some(Value::Bool(value)) => serde_json::json!({"kind": "boolean", "value": value}),
            Some(Value::Number(_)) => serde_json::json!({"kind": "number"}),
            Some(Value::String(text)) => {
                if expected.is_some_and(Value::is_string) {
                    serde_json::json!({"kind": "string", "text": QuestionInfo::diagnostic_text(text)})
                } else {
                    serde_json::json!({"kind": "string", "bytes": text.len()})
                }
            }
            Some(Value::Array(values)) => {
                serde_json::json!({"kind": "array", "count": values.len()})
            }
            Some(Value::Object(values)) => {
                let known = expected.and_then(Value::as_object).map_or(0, |fields| {
                    fields
                        .keys()
                        .filter(|key| values.contains_key(*key))
                        .count()
                });
                serde_json::json!({"kind": "object", "count": values.len(), "extra": values.len() - known})
            }
        };
        context["matches"] = Value::Bool(actual == expected);
        context
    }

    fn input(&self) -> &Value {
        match self {
            Self::Pending { input }
            | Self::Running { input }
            | Self::Completed { input, .. }
            | Self::Error { input } => input,
        }
    }
}
