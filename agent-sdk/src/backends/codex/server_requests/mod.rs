use crate::error::ErrorKind;
use serde_json::{Map, Value};

use crate::backends::codex::protocol::requests::{
    CodexCommandApprovalKind, CodexCommandApprovalRequest, CodexCommandDecision,
    CodexFileApprovalRequest, CodexMcpElicitation, CodexMcpElicitationRequest,
    CodexPermissionsApprovalRequest, CodexQuestion, CodexQuestionSet, CodexServerRequest,
    CodexUserInputRequest,
};
use crate::backends::codex::state_store::records::{
    CodexCommandDenial, CodexContinuation, CodexContinuationKind,
};
use crate::backends::codex::tools::{CodexDynamicToolCall, CodexDynamicToolCallResponse};
use crate::{ConversationNotificationKind, NotificationResolution, SdkError};

pub(crate) enum CodexServerAction {
    Interaction(CodexInteractionRequest),
    DynamicTool {
        request_id: Value,
        call: CodexDynamicToolCall,
    },
    CurrentTime {
        request_id: Value,
        thread_id: String,
    },
    Reply(CodexServerRendering),
}

#[derive(Clone)]
pub(crate) struct CodexInteractionRequest {
    request_id: Value,
    thread_id: String,
    turn_id: Option<String>,
    continuation: CodexContinuationKind,
    notification: ConversationNotificationKind,
}

pub(crate) struct CodexServerRendering {
    request_id: Value,
    outcome: CodexServerRenderingOutcome,
}

pub(crate) enum CodexServerRenderingOutcome {
    Success(Value),
    Error { code: i64, message: String },
}

struct CodexQuestionPresentation<'a> {
    questions: &'a CodexQuestionSet,
}

impl CodexServerAction {
    pub(crate) fn from_wire(request: CodexServerRequest) -> Result<Self, SdkError> {
        match request {
            CodexServerRequest::CommandApproval(request) => Self::command_approval(request),
            CodexServerRequest::FileApproval(request) => Self::file_approval(request),
            CodexServerRequest::PermissionsApproval(request) => Self::permissions_approval(request),
            CodexServerRequest::UserInput(request) => Self::user_input(request),
            CodexServerRequest::McpElicitation(request) => Self::mcp_elicitation(request),
            CodexServerRequest::DynamicTool { request_id, call } => {
                if call.has_namespace() {
                    Err(SdkError::unsupported(
                        "managed Toolsets do not use native namespaces",
                    ))
                } else {
                    Ok(Self::DynamicTool { request_id, call })
                }
            }
            CodexServerRequest::CurrentTime {
                request_id,
                thread_id,
            } => Ok(Self::CurrentTime {
                request_id,
                thread_id,
            }),
            CodexServerRequest::AuthenticationRefresh { request_id } => {
                Ok(Self::Reply(CodexServerRendering::unsupported(request_id)))
            }
            CodexServerRequest::Attestation { request_id } => Ok(Self::Reply(
                CodexServerRendering::not_yet_implemented(request_id),
            )),
        }
    }

    fn command_approval(request: CodexCommandApprovalRequest) -> Result<Self, SdkError> {
        let mut accept = false;
        let mut decline = false;
        let mut cancel = false;
        for decision in request.decisions().iter().copied() {
            match decision {
                CodexCommandDecision::Accept => accept = true,
                CodexCommandDecision::Decline => decline = true,
                CodexCommandDecision::Cancel => cancel = true,
                CodexCommandDecision::AcceptForSession
                | CodexCommandDecision::AcceptWithExecPolicyAmendment
                | CodexCommandDecision::ApplyNetworkPolicyAmendment => {}
            }
        }
        if !accept {
            return Err(SdkError::unsupported(
                "command approval has no one-shot native acceptance",
            ));
        }
        let denial = if decline {
            CodexCommandDenial::Decline
        } else if cancel {
            CodexCommandDenial::Cancel
        } else {
            return Err(SdkError::unsupported(
                "command approval has no native denial decision",
            ));
        };
        let scope = request.scope();
        let body = match request.kind() {
            CodexCommandApprovalKind::Command => CodexInteractionRequest::body(
                [request.reason(), request.command()],
                "Approve this command?",
            ),
            CodexCommandApprovalKind::WriteStdin => {
                CodexInteractionRequest::body([request.reason(), None], "Approve terminal input?")
            }
        };
        CodexInteractionRequest::new(
            scope.request_id().clone(),
            scope.thread_id().to_owned(),
            Some(scope.turn_id().to_owned()),
            CodexContinuationKind::CommandApproval {
                denial,
                dismissible: cancel,
            },
            ConversationNotificationKind::Interaction {
                title: match request.kind() {
                    CodexCommandApprovalKind::Command => "Command approval",
                    CodexCommandApprovalKind::WriteStdin => "Terminal input approval",
                }
                .to_owned(),
                body,
                response_schema: None,
                dismissible: cancel,
            },
        )
        .map(Self::Interaction)
    }

    fn file_approval(request: CodexFileApprovalRequest) -> Result<Self, SdkError> {
        let scope = request.scope();
        CodexInteractionRequest::new(
            scope.request_id().clone(),
            scope.thread_id().to_owned(),
            Some(scope.turn_id().to_owned()),
            CodexContinuationKind::FileApproval,
            ConversationNotificationKind::Interaction {
                title: "File change approval".to_owned(),
                body: CodexInteractionRequest::body(
                    [request.reason()],
                    "Approve the proposed file changes?",
                ),
                response_schema: None,
                dismissible: true,
            },
        )
        .map(Self::Interaction)
    }

    fn permissions_approval(request: CodexPermissionsApprovalRequest) -> Result<Self, SdkError> {
        let scope = request.scope();
        CodexInteractionRequest::new(
            scope.request_id().clone(),
            scope.thread_id().to_owned(),
            Some(scope.turn_id().to_owned()),
            CodexContinuationKind::PermissionsApproval {
                permissions: request.permissions().clone(),
            },
            ConversationNotificationKind::Interaction {
                title: "Permission approval".to_owned(),
                body: CodexInteractionRequest::body(
                    [request.reason()],
                    "Approve the requested permissions for this turn?",
                ),
                response_schema: None,
                dismissible: true,
            },
        )
        .map(Self::Interaction)
    }

    fn user_input(request: CodexUserInputRequest) -> Result<Self, SdkError> {
        if !request.is_blocking() {
            return Err(SdkError::unsupported(
                "nonblocking user input has no portable response policy",
            ));
        }
        let scope = request.scope();
        let presentation = CodexQuestionPresentation::new(request.questions());
        presentation.validate()?;
        CodexInteractionRequest::new(
            scope.request_id().clone(),
            scope.thread_id().to_owned(),
            Some(scope.turn_id().to_owned()),
            CodexContinuationKind::UserInput {
                question_ids: presentation.question_ids(),
            },
            ConversationNotificationKind::Interaction {
                title: presentation.title(),
                body: presentation.body(),
                response_schema: Some(presentation.schema()),
                dismissible: false,
            },
        )
        .map(Self::Interaction)
    }

    fn mcp_elicitation(request: CodexMcpElicitationRequest) -> Result<Self, SdkError> {
        match request.request() {
            CodexMcpElicitation::Form { message, schema } => CodexInteractionRequest::new(
                request.request_id().clone(),
                request.thread_id().to_owned(),
                request.turn_id().map(str::to_owned),
                CodexContinuationKind::McpForm,
                ConversationNotificationKind::Interaction {
                    title: "Input required".to_owned(),
                    body: CodexInteractionRequest::body(
                        [Some(message.as_str())],
                        "Provide the requested input.",
                    ),
                    response_schema: Some(schema.clone()),
                    dismissible: true,
                },
            )
            .map(Self::Interaction),
            CodexMcpElicitation::OpenAiForm { .. }
            | CodexMcpElicitation::Url { .. }
            | CodexMcpElicitation::UserVerification { .. } => Ok(Self::Reply(
                CodexServerRendering::unsupported(request.request_id().clone()),
            )),
        }
    }
}

impl CodexInteractionRequest {
    fn title(candidate: &str, fallback: &str) -> String {
        if candidate.is_empty()
            || candidate.trim() != candidate
            || candidate.chars().any(char::is_control)
        {
            fallback.to_owned()
        } else {
            candidate.to_owned()
        }
    }

    fn body<'a>(candidates: impl IntoIterator<Item = Option<&'a str>>, fallback: &str) -> String {
        candidates
            .into_iter()
            .flatten()
            .find(|candidate| {
                !candidate.is_empty()
                    && candidate.trim() == *candidate
                    && !candidate
                        .chars()
                        .any(|character| character.is_control() && character != '\n')
            })
            .unwrap_or(fallback)
            .to_owned()
    }

    fn new(
        request_id: Value,
        thread_id: String,
        turn_id: Option<String>,
        continuation: CodexContinuationKind,
        notification: ConversationNotificationKind,
    ) -> Result<Self, SdkError> {
        if !(request_id.as_str().is_some() || request_id.as_i64().is_some())
            || thread_id.is_empty()
            || thread_id.trim() != thread_id
            || turn_id
                .as_deref()
                .is_some_and(|turn_id| turn_id.is_empty() || turn_id.trim() != turn_id)
        {
            return Err(SdkError::protocol("interaction scope is malformed"));
        }
        let ConversationNotificationKind::Interaction { title, body, .. } = &notification else {
            return Err(SdkError::invalid_state(
                "server request did not produce one portable interaction",
            ));
        };
        if title.is_empty()
            || title.trim() != title
            || body.is_empty()
            || body.trim() != body
            || title.chars().any(char::is_control)
            || body
                .chars()
                .any(|character| character.is_control() && character != '\n')
        {
            return Err(SdkError::protocol(
                "portable interaction content is malformed",
            ));
        }
        continuation.validate_notification(&notification)?;
        Ok(Self {
            request_id,
            thread_id,
            turn_id,
            continuation,
            notification,
        })
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Value,
        String,
        Option<String>,
        CodexContinuationKind,
        ConversationNotificationKind,
    ) {
        (
            self.request_id,
            self.thread_id,
            self.turn_id,
            self.continuation,
            self.notification,
        )
    }
}

impl CodexServerRendering {
    pub(crate) fn interaction(
        continuation: &CodexContinuation,
        resolution: &NotificationResolution,
    ) -> Result<Self, SdkError> {
        let result = match (continuation.kind(), resolution) {
            (
                CodexContinuationKind::CommandApproval { .. } | CodexContinuationKind::FileApproval,
                NotificationResolution::Allow,
            ) => serde_json::json!({ "decision": "accept" }),
            (
                CodexContinuationKind::CommandApproval { denial, .. },
                NotificationResolution::Deny { .. },
            ) => serde_json::json!({
                "decision": match denial {
                    CodexCommandDenial::Decline => "decline",
                    CodexCommandDenial::Cancel => "cancel",
                }
            }),
            (
                CodexContinuationKind::CommandApproval {
                    dismissible: true, ..
                },
                NotificationResolution::Dismiss { .. },
            ) => serde_json::json!({ "decision": "cancel" }),
            (CodexContinuationKind::FileApproval, NotificationResolution::Deny { .. }) => {
                serde_json::json!({ "decision": "decline" })
            }
            (CodexContinuationKind::FileApproval, NotificationResolution::Dismiss { .. }) => {
                serde_json::json!({ "decision": "cancel" })
            }
            (
                CodexContinuationKind::PermissionsApproval { permissions },
                NotificationResolution::Allow,
            ) => serde_json::json!({
                "permissions": permissions,
                "scope": "turn"
            }),
            (
                CodexContinuationKind::PermissionsApproval { .. },
                NotificationResolution::Deny { .. } | NotificationResolution::Dismiss { .. },
            ) => serde_json::json!({
                "permissions": {},
                "scope": "turn"
            }),
            (
                CodexContinuationKind::UserInput { question_ids },
                NotificationResolution::Submit { content },
            ) => Self::user_input(question_ids, content)?,
            (CodexContinuationKind::McpForm, NotificationResolution::Submit { content }) => {
                serde_json::json!({
                    "action": "accept",
                    "content": content,
                    "_meta": null
                })
            }
            (CodexContinuationKind::McpForm, NotificationResolution::Dismiss { .. }) => {
                serde_json::json!({
                    "action": "cancel",
                    "content": null,
                    "_meta": null
                })
            }
            _ => {
                return Err(SdkError::invalid_input(
                    "notification resolution does not match the interaction",
                ));
            }
        };
        Ok(Self::success(continuation.request_id().clone(), result))
    }

    pub(crate) fn dynamic_tool(request_id: Value, response: CodexDynamicToolCallResponse) -> Self {
        Self::success(request_id, response.into_result())
    }

    pub(crate) fn current_time(request_id: Value) -> Result<Self, SdkError> {
        let seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| SdkError::integration("system clock precedes the Unix epoch"))?
            .as_secs();
        let current_time_at = i64::try_from(seconds)
            .map_err(|_| SdkError::integration("system clock exceeds the Codex wire range"))?;
        Ok(Self::success(
            request_id,
            serde_json::json!({ "currentTimeAt": current_time_at }),
        ))
    }

    pub(crate) fn from_error(request_id: Value, error: &SdkError) -> Self {
        let code = match error.kind() {
            ErrorKind::InvalidInput | ErrorKind::Protocol => -32602,
            ErrorKind::Unsupported => -32601,
            ErrorKind::InvalidConfig
            | ErrorKind::NotFound
            | ErrorKind::Conflict
            | ErrorKind::Closed
            | ErrorKind::Busy
            | ErrorKind::Cancelled
            | ErrorKind::Timeout
            | ErrorKind::NotYetImplemented
            | ErrorKind::InvalidState
            | ErrorKind::Integration => -32603,
        };
        Self::error(request_id, code, "request could not be completed")
    }

    pub(crate) fn into_parts(self) -> (Value, CodexServerRenderingOutcome) {
        (self.request_id, self.outcome)
    }

    fn success(request_id: Value, result: Value) -> Self {
        Self {
            request_id,
            outcome: CodexServerRenderingOutcome::Success(result),
        }
    }

    fn error(request_id: Value, code: i64, message: &str) -> Self {
        Self {
            request_id,
            outcome: CodexServerRenderingOutcome::Error {
                code,
                message: message.to_owned(),
            },
        }
    }

    fn unsupported(request_id: Value) -> Self {
        Self::error(request_id, -32601, "request is unsupported")
    }

    fn not_yet_implemented(request_id: Value) -> Self {
        Self::error(request_id, -32603, "request is not yet implemented")
    }

    fn user_input(question_ids: &[String], content: &Value) -> Result<Value, SdkError> {
        let answers = if question_ids.len() == 1 {
            vec![Self::answer_values(content.get("answer").ok_or_else(
                || SdkError::invalid_input("input response has no answer"),
            )?)?]
        } else {
            let values = content
                .get("answers")
                .and_then(Value::as_array)
                .filter(|answers| answers.len() == question_ids.len())
                .ok_or_else(|| {
                    SdkError::invalid_input("input response does not match the pending questions")
                })?;
            values
                .iter()
                .map(Self::answer_values)
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut native = Map::new();
        for (question_id, answers) in question_ids.iter().zip(answers) {
            native.insert(
                question_id.clone(),
                serde_json::json!({ "answers": answers }),
            );
        }
        Ok(serde_json::json!({ "answers": native }))
    }

    fn answer_values(value: &Value) -> Result<Vec<String>, SdkError> {
        value
            .as_str()
            .filter(|answer| !answer.is_empty())
            .map(|answer| vec![answer.to_owned()])
            .ok_or_else(|| SdkError::invalid_input("input answer is malformed"))
    }
}

impl<'a> CodexQuestionPresentation<'a> {
    fn new(questions: &'a CodexQuestionSet) -> Self {
        Self { questions }
    }

    fn question_ids(&self) -> Vec<String> {
        self.questions
            .questions()
            .iter()
            .map(|question| question.id().to_owned())
            .collect()
    }

    fn validate(&self) -> Result<(), SdkError> {
        let mut ids = self
            .questions
            .questions()
            .iter()
            .map(CodexQuestion::id)
            .collect::<Vec<_>>();
        if ids
            .iter()
            .any(|id| id.is_empty() || id.trim() != *id || id.chars().any(char::is_control))
        {
            return Err(SdkError::unsupported(
                "input question identity has no portable representation",
            ));
        }
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(SdkError::unsupported(
                "input question identities are not distinct",
            ));
        }
        for question in self.questions.questions() {
            let Some(options) = question.options().filter(|_| !question.allows_other()) else {
                continue;
            };
            if options.is_empty() || options.iter().any(String::is_empty) {
                return Err(SdkError::unsupported(
                    "input question has no portable answer domain",
                ));
            }
        }
        Ok(())
    }

    fn title(&self) -> String {
        match self.questions.questions() {
            [question] => CodexInteractionRequest::title(question.header(), "Input required"),
            _ => "Input required".to_owned(),
        }
    }

    fn body(&self) -> String {
        match self.questions.questions() {
            [question] => CodexInteractionRequest::body(
                [Some(question.question())],
                "Provide the requested input.",
            ),
            questions => questions
                .iter()
                .enumerate()
                .map(|(index, question)| {
                    let fallback_header = format!("Question {}", index + 1);
                    let header =
                        CodexInteractionRequest::title(question.header(), &fallback_header);
                    let question = CodexInteractionRequest::body(
                        [Some(question.question())],
                        "Provide input for this question.",
                    );
                    format!("{header}: {question}")
                })
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }

    fn schema(&self) -> Value {
        match self.questions.questions() {
            [question] => serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": { "answer": self.answer_schema(question) },
                "required": ["answer"],
                "additionalProperties": false
            }),
            questions => serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "answers": {
                        "type": "array",
                        "prefixItems": questions
                            .iter()
                            .map(|question| self.answer_schema(question))
                            .collect::<Vec<_>>(),
                        "items": false,
                        "minItems": questions.len(),
                        "maxItems": questions.len()
                    }
                },
                "required": ["answers"],
                "additionalProperties": false
            }),
        }
    }

    fn answer_schema(&self, question: &CodexQuestion) -> Value {
        let mut schema = Map::from_iter([
            ("type".to_owned(), Value::String("string".to_owned())),
            ("minLength".to_owned(), Value::from(1_u64)),
        ]);
        if let Some(options) = question.options()
            && !question.allows_other()
        {
            schema.insert(
                "enum".to_owned(),
                Value::Array(options.iter().cloned().map(Value::String).collect()),
            );
        }
        if question.is_secret() {
            schema.insert("writeOnly".to_owned(), Value::Bool(true));
        }
        Value::Object(schema)
    }
}
