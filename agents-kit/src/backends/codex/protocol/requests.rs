use serde_json::{Map, Value};

use crate::KitError;
use crate::backends::codex::adapters::prompt::CodexPrompt;
use crate::backends::codex::protocol::notifications::CodexCommandAction;
use crate::backends::codex::tools::CodexDynamicToolCall;

pub(crate) struct CodexTurnStartRequest {
    thread_id: String,
    client_message_id: String,
    text: String,
    cwd: String,
    model: Option<String>,
    reasoning_effort: Option<String>,
    output_schema: Option<Value>,
}

pub(crate) struct CodexThreadCreateRequest {
    policy: CodexThreadWirePolicy,
    instructions: Option<String>,
    thread_source: String,
    dynamic_tools: Vec<Value>,
}

pub(crate) struct CodexThreadResumeRequest {
    thread_id: String,
    policy: CodexThreadWirePolicy,
    instructions: Option<String>,
}

pub(crate) struct CodexThreadWirePolicy {
    cwd: String,
    runtime_workspace_roots: Vec<String>,
    sandbox: Option<String>,
    permissions: Option<String>,
    config: Map<String, Value>,
}

pub(crate) enum CodexServerRequest {
    CommandApproval(CodexCommandApprovalRequest),
    FileApproval(CodexFileApprovalRequest),
    PermissionsApproval(CodexPermissionsApprovalRequest),
    UserInput(CodexUserInputRequest),
    McpElicitation(CodexMcpElicitationRequest),
    DynamicTool {
        request_id: Value,
        call: CodexDynamicToolCall,
    },
    CurrentTime {
        request_id: Value,
        thread_id: String,
    },
    AuthenticationRefresh {
        request_id: Value,
    },
    Attestation {
        request_id: Value,
    },
}

pub(crate) struct CodexRequestScope {
    request_id: Value,
    thread_id: String,
    turn_id: String,
}

pub(crate) struct CodexCommandApprovalRequest {
    scope: CodexRequestScope,
    reason: Option<String>,
    command: Option<String>,
    kind: CodexCommandApprovalKind,
    decisions: Vec<CodexCommandDecision>,
}

#[derive(Clone, Copy)]
pub(crate) enum CodexCommandApprovalKind {
    Command,
    WriteStdin,
}

pub(crate) struct CodexFileApprovalRequest {
    scope: CodexRequestScope,
    reason: Option<String>,
}

pub(crate) struct CodexPermissionsApprovalRequest {
    scope: CodexRequestScope,
    reason: Option<String>,
    permissions: Value,
}

pub(crate) struct CodexUserInputRequest {
    scope: CodexRequestScope,
    questions: CodexQuestionSet,
    is_blocking: bool,
}

pub(crate) struct CodexMcpElicitationRequest {
    request_id: Value,
    thread_id: String,
    turn_id: Option<String>,
    request: CodexMcpElicitation,
}

pub(crate) enum CodexMcpElicitation {
    Form {
        message: String,
        schema: Value,
    },
    OpenAiForm {
        _message: String,
        _schema: Value,
    },
    UserVerification {
        _title: String,
        _description: String,
        _challenge: String,
    },
    Url {
        _message: String,
        _url: String,
        _elicitation_id: String,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum CodexCommandDecision {
    Accept,
    AcceptForSession,
    AcceptWithExecPolicyAmendment,
    ApplyNetworkPolicyAmendment,
    Decline,
    Cancel,
}

pub(crate) struct CodexQuestionSet {
    questions: Vec<CodexQuestion>,
}

pub(crate) struct CodexQuestion {
    id: String,
    header: String,
    question: String,
    is_other: bool,
    is_secret: bool,
    options: Option<Vec<String>>,
}

impl CodexTurnStartRequest {
    pub(crate) fn from_prompt(prompt: &CodexPrompt) -> Result<Self, KitError> {
        let cwd = prompt
            .cwd()
            .to_str()
            .ok_or_else(|| KitError::invalid_input("prompt cwd must be valid UTF-8"))?;
        Ok(Self {
            thread_id: prompt.thread_id().to_owned(),
            client_message_id: prompt.client_message_id().to_owned(),
            text: prompt.text().to_owned(),
            cwd: cwd.to_owned(),
            model: prompt.model().map(ToString::to_string),
            reasoning_effort: prompt.reasoning_effort().map(ToString::to_string),
            output_schema: prompt.output_schema().cloned(),
        })
    }

    pub(crate) fn into_params(self) -> Value {
        let mut params = Map::from_iter([
            ("threadId".to_owned(), Value::String(self.thread_id)),
            (
                "clientUserMessageId".to_owned(),
                Value::String(self.client_message_id),
            ),
            (
                "input".to_owned(),
                serde_json::json!([{
                    "type": "text",
                    "text": self.text,
                    "textElements": []
                }]),
            ),
            ("cwd".to_owned(), Value::String(self.cwd)),
        ]);
        if let Some(model) = self.model {
            params.insert("model".to_owned(), Value::String(model));
        }
        if let Some(reasoning_effort) = self.reasoning_effort {
            params.insert("effort".to_owned(), Value::String(reasoning_effort));
        }
        if let Some(output_schema) = self.output_schema {
            params.insert("outputSchema".to_owned(), output_schema);
        }
        Value::Object(params)
    }
}

impl CodexThreadCreateRequest {
    pub(crate) fn new(
        policy: CodexThreadWirePolicy,
        instructions: Option<&str>,
        thread_source: &str,
        dynamic_tools: Vec<Value>,
    ) -> Result<Self, KitError> {
        if !thread_source.starts_with("agents-kit-create-")
            || thread_source.is_empty()
            || thread_source.trim() != thread_source
        {
            return Err(KitError::invalid_state(
                "conversation source correlation is malformed",
            ));
        }
        Ok(Self {
            policy,
            instructions: instructions.map(str::to_owned),
            thread_source: thread_source.to_owned(),
            dynamic_tools,
        })
    }

    pub(crate) fn into_params(self) -> Value {
        let mut params = self.policy.into_params();
        params.insert("ephemeral".to_owned(), Value::Bool(false));
        params.insert("dynamicTools".to_owned(), Value::Array(self.dynamic_tools));
        params.insert("threadSource".to_owned(), Value::String(self.thread_source));
        if let Some(instructions) = self.instructions {
            params.insert(
                "developerInstructions".to_owned(),
                Value::String(instructions),
            );
        }
        Value::Object(params)
    }
}

impl CodexThreadResumeRequest {
    pub(crate) fn new(
        thread_id: &str,
        policy: CodexThreadWirePolicy,
        instructions: Option<&str>,
    ) -> Result<Self, KitError> {
        if thread_id.is_empty() || thread_id.trim() != thread_id {
            return Err(KitError::invalid_state(
                "durable conversation identity is malformed",
            ));
        }
        Ok(Self {
            thread_id: thread_id.to_owned(),
            policy,
            instructions: instructions.map(str::to_owned),
        })
    }

    pub(crate) fn into_params(self) -> Value {
        let mut params = self.policy.into_params();
        params.insert("threadId".to_owned(), Value::String(self.thread_id));
        params.insert("excludeTurns".to_owned(), Value::Bool(true));
        if let Some(instructions) = self.instructions {
            params.insert(
                "developerInstructions".to_owned(),
                Value::String(instructions),
            );
        }
        Value::Object(params)
    }
}

impl CodexThreadWirePolicy {
    pub(crate) fn new(
        cwd: String,
        runtime_workspace_roots: Vec<String>,
        sandbox: Option<String>,
        permissions: Option<String>,
        mut config: Map<String, Value>,
    ) -> Result<Self, KitError> {
        if cwd.is_empty() || runtime_workspace_roots.is_empty() {
            return Err(KitError::invalid_state(
                "conversation policy omitted its workspace paths",
            ));
        }
        if sandbox.is_some() == permissions.is_some() {
            return Err(KitError::invalid_state(
                "conversation policy must select exactly one permission representation",
            ));
        }
        if sandbox.as_deref().is_some_and(|sandbox| {
            !matches!(
                sandbox,
                "read-only" | "workspace-write" | "danger-full-access"
            )
        }) {
            return Err(KitError::invalid_state(
                "conversation workspace execution policy selection is malformed",
            ));
        }
        if permissions.as_deref().is_some_and(|selection| {
            selection.is_empty()
                || selection.trim() != selection
                || !selection
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-'))
        }) {
            return Err(KitError::invalid_state(
                "conversation workspace execution policy selection is malformed",
            ));
        }
        config.insert("features.multi_agent".to_owned(), Value::Bool(false));
        config.insert("features.multi_agent_v2".to_owned(), Value::Bool(false));
        config.insert(
            "suppress_unstable_features_warning".to_owned(),
            Value::Bool(true),
        );
        Ok(Self {
            cwd,
            runtime_workspace_roots,
            sandbox,
            permissions,
            config,
        })
    }

    fn into_params(self) -> Map<String, Value> {
        let mut params = Map::from_iter([
            ("cwd".to_owned(), Value::String(self.cwd)),
            (
                "runtimeWorkspaceRoots".to_owned(),
                Value::Array(
                    self.runtime_workspace_roots
                        .into_iter()
                        .map(Value::String)
                        .collect(),
                ),
            ),
            ("config".to_owned(), Value::Object(self.config)),
        ]);
        if let Some(sandbox) = self.sandbox {
            params.insert("sandbox".to_owned(), Value::String(sandbox));
        }
        if let Some(permissions) = self.permissions {
            params.insert("permissions".to_owned(), Value::String(permissions));
        }
        params
    }
}

impl CodexServerRequest {
    pub(crate) fn decode(request_id: Value, method: &str, params: Value) -> Result<Self, KitError> {
        Self::validate_request_id(&request_id)?;
        match method {
            "item/commandExecution/requestApproval" => {
                Self::command_approval(request_id, params).map(Self::CommandApproval)
            }
            "item/fileChange/requestApproval" => {
                Self::file_approval(request_id, params).map(Self::FileApproval)
            }
            "item/permissions/requestApproval" => {
                Self::permissions_approval(request_id, params).map(Self::PermissionsApproval)
            }
            "item/tool/requestUserInput" => {
                Self::user_input(request_id, params).map(Self::UserInput)
            }
            "mcpServer/elicitation/request" => {
                Self::mcp_elicitation(request_id, params).map(Self::McpElicitation)
            }
            "item/tool/call" => Ok(Self::DynamicTool {
                request_id,
                call: CodexDynamicToolCall::decode(params)?,
            }),
            "currentTime/read" => {
                Self::require_object(&params)?;
                Ok(Self::CurrentTime {
                    thread_id: Self::thread_id(&params)?.to_owned(),
                    request_id,
                })
            }
            "account/chatgptAuthTokens/refresh" => {
                Self::require_object(&params)?;
                if params.get("reason").and_then(Value::as_str) != Some("unauthorized") {
                    return Err(KitError::protocol(
                        "authentication refresh reason is malformed",
                    ));
                }
                if let Some(previous) = params.get("previousAccountId")
                    && !previous.is_null()
                {
                    Self::plain_string(previous, "previousAccountId")?;
                }
                Ok(Self::AuthenticationRefresh { request_id })
            }
            "attestation/generate" => {
                Self::require_object(&params)?;
                Ok(Self::Attestation { request_id })
            }
            _ => Err(KitError::unsupported(
                "server process request method is not selected",
            )),
        }
    }

    pub(crate) fn request_id(&self) -> &Value {
        match self {
            Self::CommandApproval(request) => request.scope.request_id(),
            Self::FileApproval(request) => request.scope.request_id(),
            Self::PermissionsApproval(request) => request.scope.request_id(),
            Self::UserInput(request) => request.scope.request_id(),
            Self::McpElicitation(request) => &request.request_id,
            Self::DynamicTool { request_id, .. }
            | Self::CurrentTime { request_id, .. }
            | Self::AuthenticationRefresh { request_id }
            | Self::Attestation { request_id } => request_id,
        }
    }

    fn command_approval(
        request_id: Value,
        params: Value,
    ) -> Result<CodexCommandApprovalRequest, KitError> {
        let thread_id = Self::thread_id(&params)?.to_owned();
        let turn_id = Self::required_string(&params, "turnId")?.to_owned();
        Self::required_string(&params, "itemId")?;
        Self::required_i64(&params, "startedAtMs")?;
        Self::optional_plain_string(&params, "approvalId")?;
        Self::optional_plain_string(&params, "environmentId")?;
        let reason = Self::optional_plain_owned_string(&params, "reason")?;
        let command = Self::optional_plain_owned_string(&params, "command")?;
        Self::optional_plain_string(&params, "cwd")?;
        Self::optional_network_approval_context(&params)?;
        Self::optional_command_actions(&params)?;
        Self::optional_permission_profile(&params, "additionalPermissions", false)?;
        Self::optional_execpolicy_amendment(&params, "proposedExecpolicyAmendment")?;
        Self::optional_network_policy_amendments(&params)?;
        Ok(CodexCommandApprovalRequest {
            scope: CodexRequestScope::new(request_id, thread_id, turn_id),
            reason,
            command,
            kind: match params.get("kind") {
                None => CodexCommandApprovalKind::Command,
                Some(Value::String(kind)) if kind == "command" => CodexCommandApprovalKind::Command,
                Some(Value::String(kind)) if kind == "writeStdin" => {
                    CodexCommandApprovalKind::WriteStdin
                }
                Some(_) => return Err(KitError::protocol("command approval kind is malformed")),
            },
            decisions: Self::command_decisions(&params)?,
        })
    }

    fn file_approval(
        request_id: Value,
        params: Value,
    ) -> Result<CodexFileApprovalRequest, KitError> {
        let thread_id = Self::thread_id(&params)?.to_owned();
        let turn_id = Self::required_string(&params, "turnId")?.to_owned();
        Self::required_string(&params, "itemId")?;
        Self::required_i64(&params, "startedAtMs")?;
        let reason = Self::optional_plain_owned_string(&params, "reason")?;
        Self::optional_plain_string(&params, "grantRoot")?;
        Ok(CodexFileApprovalRequest {
            scope: CodexRequestScope::new(request_id, thread_id, turn_id),
            reason,
        })
    }

    fn permissions_approval(
        request_id: Value,
        params: Value,
    ) -> Result<CodexPermissionsApprovalRequest, KitError> {
        let thread_id = Self::thread_id(&params)?.to_owned();
        let turn_id = Self::required_string(&params, "turnId")?.to_owned();
        Self::required_string(&params, "itemId")?;
        Self::required_i64(&params, "startedAtMs")?;
        let cwd = Self::plain_required_string(&params, "cwd")?;
        Self::optional_plain_string(&params, "environmentId")?;
        let reason = Self::optional_plain_owned_string(&params, "reason")?;
        let permissions = params
            .get("permissions")
            .filter(|value| Self::permission_profile_is_valid(value, true))
            .cloned()
            .ok_or_else(|| KitError::protocol("permission request is malformed"))?;
        Self::validate_permission_location(cwd)?;
        if !std::path::Path::new(cwd).is_absolute()
            || !Self::permission_context_is_local(&permissions)
        {
            return Err(KitError::unsupported(
                "permission request requires an unsupported execution context",
            ));
        }
        Ok(CodexPermissionsApprovalRequest {
            scope: CodexRequestScope::new(request_id, thread_id, turn_id),
            reason,
            permissions,
        })
    }

    fn validate_permission_location(cwd: &str) -> Result<(), KitError> {
        if cwd.contains('\0') {
            return Err(KitError::protocol("permission request cwd is malformed"));
        }
        // LegacyAppPathString keeps foreign executor spellings intact. A valid
        // location outside this host's filesystem cannot authorize local work.
        let windows_drive = cwd.as_bytes().get(0..3).is_some_and(|prefix| {
            prefix[0].is_ascii_alphabetic()
                && prefix[1] == b':'
                && matches!(prefix[2], b'/' | b'\\')
        });
        let windows_unc = cwd
            .as_bytes()
            .get(..2)
            .is_some_and(|prefix| prefix.iter().all(|byte| matches!(byte, b'/' | b'\\')));
        if (windows_drive || windows_unc) && std::path::MAIN_SEPARATOR == '/' {
            if windows_unc {
                let mut components = cwd[2..].split(['/', '\\']);
                let host = components.next();
                let share = components.next();
                // Native device namespaces retain opaque Windows spellings.
                // Ordinary UNC paths require both their host and share root.
                let namespace = matches!(host, Some("." | "?")) && share.is_some();
                if !namespace && (host.is_none_or(str::is_empty) || share.is_none_or(str::is_empty))
                {
                    return Err(KitError::protocol("permission request cwd is malformed"));
                }
            }
            return Err(KitError::unsupported(
                "permission location is not a local workspace path",
            ));
        }
        if cwd.starts_with("file:") {
            let uri = reqwest::Url::parse(cwd)
                .map_err(|_| KitError::protocol("permission request cwd is malformed"))?;
            if !uri.username().is_empty()
                || uri.password().is_some()
                || uri.port().is_some()
                || uri.query().is_some()
                || uri.fragment().is_some()
                || urlencoding::decode_binary(uri.path().as_bytes()).contains(&0)
            {
                return Err(KitError::protocol("permission request cwd is malformed"));
            }
            return Err(KitError::unsupported(
                "permission location is not a local workspace path",
            ));
        }
        Ok(())
    }

    fn user_input(request_id: Value, params: Value) -> Result<CodexUserInputRequest, KitError> {
        let thread_id = Self::thread_id(&params)?.to_owned();
        let turn_id = Self::required_string(&params, "turnId")?.to_owned();
        Self::required_string(&params, "itemId")?;
        let _auto_resolution_ms = match params.get("autoResolutionMs") {
            None | Some(Value::Null) => None,
            Some(timeout) => Some(timeout.as_u64().ok_or_else(|| {
                KitError::protocol("input auto-resolution deadline is malformed")
            })?),
        };
        let questions = CodexQuestionSet::decode(
            params
                .get("questions")
                .ok_or_else(|| KitError::protocol("input request has no questions"))?,
        )?;
        Ok(CodexUserInputRequest {
            scope: CodexRequestScope::new(request_id, thread_id, turn_id),
            questions,
            is_blocking: match params.get("isBlocking") {
                None | Some(Value::Null) => true,
                Some(Value::Bool(blocking)) => *blocking,
                Some(_) => return Err(KitError::protocol("input blocking policy is malformed")),
            },
        })
    }

    fn mcp_elicitation(
        request_id: Value,
        params: Value,
    ) -> Result<CodexMcpElicitationRequest, KitError> {
        let thread_id = Self::thread_id(&params)?.to_owned();
        let turn_id = match params.get("turnId") {
            None | Some(Value::Null) => None,
            Some(value) => Some(Self::nonempty_string(value, "turnId")?.to_owned()),
        };
        Self::required_string(&params, "serverName")?;
        let request = match params.get("mode").and_then(Value::as_str) {
            Some("form") => {
                let message = Self::plain_required_string(&params, "message")?.to_owned();
                let schema = params
                    .get("requestedSchema")
                    .ok_or_else(|| KitError::protocol("MCP form schema is malformed"))
                    .and_then(Self::canonical_mcp_form_schema)?;
                CodexMcpElicitation::Form { message, schema }
            }
            Some("openai/form" | "openaiForm") => {
                let message = Self::plain_required_string(&params, "message")?.to_owned();
                let schema = params
                    .get("requestedSchema")
                    .cloned()
                    .ok_or_else(|| KitError::protocol("MCP form schema is malformed"))?;
                CodexMcpElicitation::OpenAiForm {
                    _message: message,
                    _schema: schema,
                }
            }
            Some("openai/userVerification") => CodexMcpElicitation::UserVerification {
                _title: Self::plain_required_string(&params, "title")?.to_owned(),
                _description: Self::plain_required_string(&params, "description")?.to_owned(),
                _challenge: Self::plain_required_string(&params, "challenge")?.to_owned(),
            },
            Some("url") => CodexMcpElicitation::Url {
                _message: Self::plain_required_string(&params, "message")?.to_owned(),
                _url: Self::plain_required_string(&params, "url")?.to_owned(),
                _elicitation_id: Self::plain_required_string(&params, "elicitationId")?.to_owned(),
            },
            _ => {
                return Err(KitError::protocol("MCP elicitation mode is malformed"));
            }
        };
        Ok(CodexMcpElicitationRequest {
            request_id,
            thread_id,
            turn_id,
            request,
        })
    }

    fn validate_request_id(request_id: &Value) -> Result<(), KitError> {
        if request_id.as_str().is_some() || request_id.as_i64().is_some() {
            Ok(())
        } else {
            Err(KitError::protocol(
                "JSON-RPC request id must be a string or signed integer",
            ))
        }
    }

    fn thread_id(params: &Value) -> Result<&str, KitError> {
        Self::required_string(params, "threadId")
    }

    fn required_string<'a>(params: &'a Value, field: &str) -> Result<&'a str, KitError> {
        params
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.trim() == *value)
            .ok_or_else(|| KitError::protocol(format!("request `{field}` is malformed")))
    }

    fn nonempty_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, KitError> {
        value
            .as_str()
            .filter(|value| !value.is_empty() && value.trim() == *value)
            .ok_or_else(|| KitError::protocol(format!("request `{field}` is malformed")))
    }

    fn plain_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, KitError> {
        value
            .as_str()
            .ok_or_else(|| KitError::protocol(format!("request `{field}` is malformed")))
    }

    fn plain_required_string<'a>(params: &'a Value, field: &str) -> Result<&'a str, KitError> {
        params
            .get(field)
            .ok_or_else(|| KitError::protocol(format!("request `{field}` is missing")))
            .and_then(|value| Self::plain_string(value, field))
    }

    fn required_i64(params: &Value, field: &str) -> Result<i64, KitError> {
        params
            .get(field)
            .and_then(Value::as_i64)
            .ok_or_else(|| KitError::protocol(format!("request `{field}` is malformed")))
    }

    fn optional_plain_string(params: &Value, field: &str) -> Result<(), KitError> {
        match params.get(field) {
            None | Some(Value::Null) => Ok(()),
            Some(value) => Self::plain_string(value, field).map(|_| ()),
        }
    }

    fn optional_plain_owned_string(
        params: &Value,
        field: &str,
    ) -> Result<Option<String>, KitError> {
        match params.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => Self::plain_string(value, field)
                .map(str::to_owned)
                .map(Some),
        }
    }

    fn require_object(params: &Value) -> Result<&Map<String, Value>, KitError> {
        params
            .as_object()
            .ok_or_else(|| KitError::protocol("request params are malformed"))
    }

    fn command_decisions(params: &Value) -> Result<Vec<CodexCommandDecision>, KitError> {
        let decisions = match params.get("availableDecisions") {
            None | Some(Value::Null) => return Ok(Self::default_command_decisions(params)),
            Some(Value::Array(decisions)) => decisions,
            Some(_) => {
                return Err(KitError::protocol("command decisions are malformed"));
            }
        };
        decisions.iter().map(Self::command_decision).collect()
    }

    fn command_decision(decision: &Value) -> Result<CodexCommandDecision, KitError> {
        match decision.as_str() {
            Some("accept") => Ok(CodexCommandDecision::Accept),
            Some("acceptForSession") => Ok(CodexCommandDecision::AcceptForSession),
            Some("decline") => Ok(CodexCommandDecision::Decline),
            Some("cancel") => Ok(CodexCommandDecision::Cancel),
            Some(_) => Err(KitError::protocol("command decision is malformed")),
            None => {
                let object = decision
                    .as_object()
                    .filter(|object| object.len() == 1)
                    .ok_or_else(|| KitError::protocol("command decision is malformed"))?;
                if let Some(value) = object.get("acceptWithExecpolicyAmendment") {
                    let amendment = value
                        .get("execpolicy_amendment")
                        .filter(|amendment| Self::execpolicy_amendment_is_valid(amendment));
                    return amendment
                        .map(|_| CodexCommandDecision::AcceptWithExecPolicyAmendment)
                        .ok_or_else(|| KitError::protocol("execpolicy decision is malformed"));
                }
                if let Some(value) = object.get("applyNetworkPolicyAmendment") {
                    let amendment = value
                        .get("network_policy_amendment")
                        .filter(|amendment| Self::network_policy_amendment_is_valid(amendment));
                    return amendment
                        .map(|_| CodexCommandDecision::ApplyNetworkPolicyAmendment)
                        .ok_or_else(|| KitError::protocol("network policy decision is malformed"));
                }
                Err(KitError::protocol("command decision is malformed"))
            }
        }
    }

    fn default_command_decisions(params: &Value) -> Vec<CodexCommandDecision> {
        if params
            .get("networkApprovalContext")
            .is_some_and(|value| !value.is_null())
        {
            let mut decisions = vec![
                CodexCommandDecision::Accept,
                CodexCommandDecision::AcceptForSession,
            ];
            if params
                .get("proposedNetworkPolicyAmendments")
                .and_then(Value::as_array)
                .is_some_and(|amendments| {
                    amendments.iter().any(|amendment| {
                        amendment.get("action").and_then(Value::as_str) == Some("allow")
                    })
                })
            {
                decisions.push(CodexCommandDecision::ApplyNetworkPolicyAmendment);
            }
            decisions.push(CodexCommandDecision::Cancel);
            return decisions;
        }
        if params
            .get("additionalPermissions")
            .is_some_and(|value| !value.is_null())
        {
            return vec![CodexCommandDecision::Accept, CodexCommandDecision::Cancel];
        }
        let mut decisions = vec![CodexCommandDecision::Accept];
        if params
            .get("proposedExecpolicyAmendment")
            .is_some_and(|value| !value.is_null())
        {
            decisions.push(CodexCommandDecision::AcceptWithExecPolicyAmendment);
        }
        decisions.push(CodexCommandDecision::Cancel);
        decisions
    }

    fn optional_network_approval_context(params: &Value) -> Result<(), KitError> {
        let Some(value) = params.get("networkApprovalContext") else {
            return Ok(());
        };
        if value.is_null() {
            return Ok(());
        }
        let valid = value.as_object().is_some_and(|context| {
            context.get("host").is_some_and(Value::is_string)
                && context
                    .get("protocol")
                    .and_then(Value::as_str)
                    .is_some_and(|protocol| {
                        matches!(protocol, "http" | "https" | "socks5Tcp" | "socks5Udp")
                    })
        });
        valid
            .then_some(())
            .ok_or_else(|| KitError::protocol("network approval context is malformed"))
    }

    fn optional_command_actions(params: &Value) -> Result<(), KitError> {
        let Some(value) = params.get("commandActions") else {
            return Ok(());
        };
        if value.is_null() {
            return Ok(());
        }
        serde_json::from_value::<Vec<CodexCommandAction>>(value.clone())
            .map(|_| ())
            .map_err(|_| KitError::protocol("command actions are malformed"))
    }

    fn optional_permission_profile(
        params: &Value,
        field: &str,
        deny_unknown_fields: bool,
    ) -> Result<(), KitError> {
        let Some(value) = params.get(field) else {
            return Ok(());
        };
        if value.is_null() || Self::permission_profile_is_valid(value, deny_unknown_fields) {
            Ok(())
        } else {
            Err(KitError::protocol(format!(
                "request `{field}` is malformed"
            )))
        }
    }

    fn optional_execpolicy_amendment(params: &Value, field: &str) -> Result<(), KitError> {
        let Some(value) = params.get(field) else {
            return Ok(());
        };
        if value.is_null() || Self::execpolicy_amendment_is_valid(value) {
            Ok(())
        } else {
            Err(KitError::protocol("execpolicy amendment is malformed"))
        }
    }

    fn execpolicy_amendment_is_valid(value: &Value) -> bool {
        value
            .as_array()
            .is_some_and(|command| command.iter().all(Value::is_string))
    }

    fn optional_network_policy_amendments(params: &Value) -> Result<(), KitError> {
        let Some(value) = params.get("proposedNetworkPolicyAmendments") else {
            return Ok(());
        };
        if value.is_null()
            || value.as_array().is_some_and(|amendments| {
                amendments
                    .iter()
                    .all(Self::network_policy_amendment_is_valid)
            })
        {
            Ok(())
        } else {
            Err(KitError::protocol(
                "network policy amendments are malformed",
            ))
        }
    }

    fn network_policy_amendment_is_valid(value: &Value) -> bool {
        value.as_object().is_some_and(|amendment| {
            amendment.get("host").is_some_and(Value::is_string)
                && amendment
                    .get("action")
                    .and_then(Value::as_str)
                    .is_some_and(|action| matches!(action, "allow" | "deny"))
        })
    }

    fn permission_profile_is_valid(value: &Value, deny_unknown_fields: bool) -> bool {
        let Some(profile) = value.as_object() else {
            return false;
        };
        if deny_unknown_fields
            && profile
                .keys()
                .any(|key| !matches!(key.as_str(), "network" | "fileSystem"))
        {
            return false;
        }
        let network_valid = profile.get("network").is_none_or(|network| {
            network.is_null()
                || network.as_object().is_some_and(|network| {
                    network
                        .get("enabled")
                        .is_none_or(|enabled| enabled.is_null() || enabled.is_boolean())
                })
        });
        let file_system_valid = profile.get("fileSystem").is_none_or(|file_system| {
            file_system.is_null() || Self::file_permissions_are_valid(file_system)
        });
        network_valid && file_system_valid
    }

    fn permission_context_is_local(permissions: &Value) -> bool {
        let Some(file_system) = permissions
            .get("fileSystem")
            .filter(|value| !value.is_null())
        else {
            return true;
        };
        let roots = ["read", "write"]
            .into_iter()
            .filter_map(|field| file_system.get(field).and_then(Value::as_array))
            .flatten()
            .filter_map(Value::as_str);
        if !roots
            .into_iter()
            .all(|path| std::path::Path::new(path).is_absolute())
        {
            return false;
        }
        file_system
            .get("entries")
            .and_then(Value::as_array)
            .is_none_or(|entries| {
                entries.iter().all(|entry| {
                    let path = &entry["path"];
                    match path["type"].as_str() {
                        Some("path") => path["path"]
                            .as_str()
                            .is_some_and(|path| std::path::Path::new(path).is_absolute()),
                        Some("glob_pattern" | "special") => true,
                        _ => false,
                    }
                })
            })
    }

    fn file_permissions_are_valid(value: &Value) -> bool {
        let Some(permissions) = value.as_object() else {
            return false;
        };
        let roots_valid = ["read", "write"].into_iter().all(|field| {
            permissions.get(field).is_none_or(|roots| {
                roots.is_null()
                    || roots
                        .as_array()
                        .is_some_and(|roots| roots.iter().all(Value::is_string))
            })
        });
        let depth_valid = permissions.get("globScanMaxDepth").is_none_or(|depth| {
            depth.is_null()
                || depth
                    .as_u64()
                    .is_some_and(|depth| depth != 0 && usize::try_from(depth).is_ok())
        });
        let entries_valid = permissions.get("entries").is_none_or(|entries| {
            entries.is_null()
                || entries
                    .as_array()
                    .is_some_and(|entries| entries.iter().all(Self::permission_entry_is_valid))
        });
        roots_valid && depth_valid && entries_valid
    }

    fn permission_entry_is_valid(value: &Value) -> bool {
        let Some(entry) = value.as_object() else {
            return false;
        };
        entry
            .get("access")
            .and_then(Value::as_str)
            .is_some_and(|access| matches!(access, "read" | "write" | "deny"))
            && entry
                .get("path")
                .is_some_and(Self::permission_path_is_valid)
    }

    fn permission_path_is_valid(value: &Value) -> bool {
        let Some(path) = value.as_object() else {
            return false;
        };
        match path.get("type").and_then(Value::as_str) {
            Some("path") => path.get("path").is_some_and(Value::is_string),
            Some("glob_pattern") => path.get("pattern").is_some_and(Value::is_string),
            Some("special") => path
                .get("value")
                .is_some_and(Self::special_permission_path_is_valid),
            _ => false,
        }
    }

    fn special_permission_path_is_valid(value: &Value) -> bool {
        let Some(path) = value.as_object() else {
            return false;
        };
        let optional_subpath = path
            .get("subpath")
            .is_none_or(|subpath| subpath.is_null() || subpath.is_string());
        match path.get("kind").and_then(Value::as_str) {
            Some("root" | "minimal" | "tmpdir" | "slash_tmp") => true,
            Some("project_roots" | "current_working_directory") => optional_subpath,
            Some("unknown") => path.get("path").is_some_and(Value::is_string) && optional_subpath,
            _ => false,
        }
    }

    fn mcp_form_schema_is_valid(value: &Value) -> bool {
        let Some(schema) = value.as_object() else {
            return false;
        };
        Self::has_only_keys(schema, &["$schema", "type", "properties", "required"])
            && Self::optional_string_value(schema, "$schema")
            && schema.get("type").and_then(Value::as_str) == Some("object")
            && schema.get("properties").is_some_and(|properties| {
                properties.as_object().is_some_and(|properties| {
                    properties.values().all(Self::mcp_primitive_schema_is_valid)
                })
            })
            && schema.get("required").is_none_or(|required| {
                required.is_null()
                    || required
                        .as_array()
                        .is_some_and(|required| required.iter().all(Value::is_string))
            })
    }

    fn canonical_mcp_form_schema(value: &Value) -> Result<Value, KitError> {
        let mut canonical = value.clone();
        let schema = canonical
            .as_object_mut()
            .ok_or_else(|| KitError::protocol("MCP form schema is malformed"))?;
        Self::omit_null_schema_fields(schema, &["$schema", "required"]);
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            let mut names = required
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>();
            names.sort_unstable();
            if names.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(KitError::protocol(
                    "MCP form schema has duplicate required properties",
                ));
            }
        }
        if let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) {
            for property in properties.values_mut() {
                Self::canonicalize_mcp_primitive_schema(property)?;
            }
        }
        if !Self::mcp_form_schema_is_valid(&canonical) {
            return Err(KitError::protocol("MCP form schema is malformed"));
        }
        jsonschema::draft202012::meta::validate(&canonical)
            .map_err(|_| KitError::protocol("MCP form schema is malformed"))?;
        Ok(canonical)
    }

    fn canonicalize_mcp_primitive_schema(value: &mut Value) -> Result<(), KitError> {
        let Some(schema) = value.as_object_mut() else {
            return Ok(());
        };
        match schema.get("type").and_then(Value::as_str) {
            Some("string") => {
                Self::omit_null_schema_fields(schema, &["title", "description", "default"]);
                if schema.contains_key("enum") {
                    Self::omit_null_schema_fields(schema, &["enumNames"]);
                } else if !schema.contains_key("oneOf") {
                    Self::omit_null_schema_fields(schema, &["minLength", "maxLength", "format"]);
                }
            }
            Some("number" | "integer") => Self::omit_null_schema_fields(
                schema,
                &["title", "description", "minimum", "maximum", "default"],
            ),
            Some("boolean") => {
                Self::omit_null_schema_fields(schema, &["title", "description", "default"]);
            }
            Some("array") => {
                Self::omit_null_schema_fields(
                    schema,
                    &["title", "description", "minItems", "maxItems", "default"],
                );
                if let Some(items) = schema.get_mut("items").and_then(Value::as_object_mut) {
                    if items.contains_key("anyOf") && items.contains_key("oneOf") {
                        return Err(KitError::protocol(
                            "MCP form schema has conflicting titled array item aliases",
                        ));
                    }
                    if let Some(options) = items.remove("oneOf") {
                        items.insert("anyOf".to_owned(), options);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn omit_null_schema_fields(schema: &mut Map<String, Value>, fields: &[&str]) {
        for field in fields {
            if schema.get(*field).is_some_and(Value::is_null) {
                schema.remove(*field);
            }
        }
    }

    fn mcp_primitive_schema_is_valid(value: &Value) -> bool {
        let Some(schema) = value.as_object() else {
            return false;
        };
        match schema.get("type").and_then(Value::as_str) {
            Some("string") => Self::mcp_string_schema_is_valid(schema),
            Some("number" | "integer") => Self::mcp_number_schema_is_valid(schema),
            Some("boolean") => Self::mcp_boolean_schema_is_valid(schema),
            Some("array") => Self::mcp_array_schema_is_valid(schema),
            _ => false,
        }
    }

    fn mcp_string_schema_is_valid(schema: &Map<String, Value>) -> bool {
        let common_valid = Self::optional_string_value(schema, "title")
            && Self::optional_string_value(schema, "description")
            && Self::optional_string_value(schema, "default");
        if !common_valid {
            return false;
        }
        if schema.contains_key("oneOf") {
            return Self::has_only_keys(
                schema,
                &["type", "title", "description", "oneOf", "default"],
            ) && schema
                .get("oneOf")
                .and_then(Value::as_array)
                .is_some_and(|options| options.iter().all(Self::mcp_const_option_is_valid));
        }
        if schema.contains_key("enum") {
            return Self::has_only_keys(
                schema,
                &[
                    "type",
                    "title",
                    "description",
                    "enum",
                    "enumNames",
                    "default",
                ],
            ) && schema
                .get("enum")
                .and_then(Value::as_array)
                .is_some_and(|options| options.iter().all(Value::is_string))
                && schema.get("enumNames").is_none_or(|names| {
                    names.is_null()
                        || names
                            .as_array()
                            .is_some_and(|names| names.iter().all(Value::is_string))
                });
        }
        Self::has_only_keys(
            schema,
            &[
                "type",
                "title",
                "description",
                "minLength",
                "maxLength",
                "format",
                "default",
            ],
        ) && ["minLength", "maxLength"].into_iter().all(|field| {
            schema.get(field).is_none_or(|value| {
                value.is_null()
                    || value
                        .as_u64()
                        .is_some_and(|value| u32::try_from(value).is_ok())
            })
        }) && schema.get("format").is_none_or(|format| {
            format.is_null()
                || format
                    .as_str()
                    .is_some_and(|format| matches!(format, "email" | "uri" | "date" | "date-time"))
        })
    }

    fn mcp_number_schema_is_valid(schema: &Map<String, Value>) -> bool {
        Self::has_only_keys(
            schema,
            &[
                "type",
                "title",
                "description",
                "minimum",
                "maximum",
                "default",
            ],
        ) && Self::optional_string_value(schema, "title")
            && Self::optional_string_value(schema, "description")
            && ["minimum", "maximum", "default"].into_iter().all(|field| {
                schema
                    .get(field)
                    .is_none_or(|value| value.is_null() || value.as_f64().is_some())
            })
    }

    fn mcp_boolean_schema_is_valid(schema: &Map<String, Value>) -> bool {
        Self::has_only_keys(schema, &["type", "title", "description", "default"])
            && Self::optional_string_value(schema, "title")
            && Self::optional_string_value(schema, "description")
            && schema
                .get("default")
                .is_none_or(|value| value.is_null() || value.is_boolean())
    }

    fn mcp_array_schema_is_valid(schema: &Map<String, Value>) -> bool {
        Self::has_only_keys(
            schema,
            &[
                "type",
                "title",
                "description",
                "minItems",
                "maxItems",
                "items",
                "default",
            ],
        ) && Self::optional_string_value(schema, "title")
            && Self::optional_string_value(schema, "description")
            && ["minItems", "maxItems"].into_iter().all(|field| {
                schema
                    .get(field)
                    .is_none_or(|value| value.is_null() || value.as_u64().is_some())
            })
            && schema
                .get("items")
                .is_some_and(Self::mcp_array_items_are_valid)
            && schema.get("default").is_none_or(|default| {
                default.is_null()
                    || default
                        .as_array()
                        .is_some_and(|values| values.iter().all(Value::is_string))
            })
    }

    fn mcp_array_items_are_valid(value: &Value) -> bool {
        let Some(items) = value.as_object() else {
            return false;
        };
        if items.contains_key("anyOf") || items.contains_key("oneOf") {
            let field = if items.contains_key("anyOf") && !items.contains_key("oneOf") {
                "anyOf"
            } else if items.contains_key("oneOf") && !items.contains_key("anyOf") {
                "oneOf"
            } else {
                return false;
            };
            return Self::has_only_keys(items, &[field])
                && items
                    .get(field)
                    .and_then(Value::as_array)
                    .is_some_and(|options| options.iter().all(Self::mcp_const_option_is_valid));
        }
        Self::has_only_keys(items, &["type", "enum"])
            && items.get("type").and_then(Value::as_str) == Some("string")
            && items
                .get("enum")
                .and_then(Value::as_array)
                .is_some_and(|options| options.iter().all(Value::is_string))
    }

    fn mcp_const_option_is_valid(value: &Value) -> bool {
        value.as_object().is_some_and(|option| {
            Self::has_only_keys(option, &["const", "title"])
                && option.get("const").is_some_and(Value::is_string)
                && option.get("title").is_some_and(Value::is_string)
        })
    }

    fn optional_string_value(object: &Map<String, Value>, field: &str) -> bool {
        object
            .get(field)
            .is_none_or(|value| value.is_null() || value.is_string())
    }

    fn has_only_keys(object: &Map<String, Value>, allowed: &[&str]) -> bool {
        object.keys().all(|key| allowed.contains(&key.as_str()))
    }
}

impl CodexRequestScope {
    fn new(request_id: Value, thread_id: String, turn_id: String) -> Self {
        Self {
            request_id,
            thread_id,
            turn_id,
        }
    }

    pub(crate) fn request_id(&self) -> &Value {
        &self.request_id
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }
}

impl CodexCommandApprovalRequest {
    pub(crate) fn kind(&self) -> CodexCommandApprovalKind {
        self.kind
    }

    pub(crate) fn scope(&self) -> &CodexRequestScope {
        &self.scope
    }

    pub(crate) fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    pub(crate) fn command(&self) -> Option<&str> {
        self.command.as_deref()
    }

    pub(crate) fn decisions(&self) -> &[CodexCommandDecision] {
        &self.decisions
    }
}

impl CodexFileApprovalRequest {
    pub(crate) fn scope(&self) -> &CodexRequestScope {
        &self.scope
    }

    pub(crate) fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

impl CodexPermissionsApprovalRequest {
    pub(crate) fn scope(&self) -> &CodexRequestScope {
        &self.scope
    }

    pub(crate) fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    pub(crate) fn permissions(&self) -> &Value {
        &self.permissions
    }
}

impl CodexUserInputRequest {
    pub(crate) fn scope(&self) -> &CodexRequestScope {
        &self.scope
    }

    pub(crate) fn questions(&self) -> &CodexQuestionSet {
        &self.questions
    }

    pub(crate) fn is_blocking(&self) -> bool {
        self.is_blocking
    }
}

impl CodexMcpElicitationRequest {
    pub(crate) fn request_id(&self) -> &Value {
        &self.request_id
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn turn_id(&self) -> Option<&str> {
        self.turn_id.as_deref()
    }

    pub(crate) fn request(&self) -> &CodexMcpElicitation {
        &self.request
    }
}

impl CodexQuestionSet {
    fn decode(value: &Value) -> Result<Self, KitError> {
        let values = value
            .as_array()
            .filter(|values| !values.is_empty())
            .ok_or_else(|| KitError::protocol("input questions are malformed"))?;
        let questions = values
            .iter()
            .map(CodexQuestion::decode)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { questions })
    }

    pub(crate) fn questions(&self) -> &[CodexQuestion] {
        &self.questions
    }
}

impl CodexQuestion {
    fn decode(value: &Value) -> Result<Self, KitError> {
        let id = CodexServerRequest::plain_required_string(value, "id")?.to_owned();
        let header = CodexServerRequest::plain_required_string(value, "header")?.to_owned();
        let question = CodexServerRequest::plain_required_string(value, "question")?.to_owned();
        let is_other = match value.get("isOther") {
            None => false,
            Some(value) => value
                .as_bool()
                .ok_or_else(|| KitError::protocol("input isOther is malformed"))?,
        };
        let is_secret = match value.get("isSecret") {
            None => false,
            Some(value) => value
                .as_bool()
                .ok_or_else(|| KitError::protocol("input isSecret is malformed"))?,
        };
        let options = match value.get("options") {
            None | Some(Value::Null) => None,
            Some(Value::Array(options)) => Some(
                options
                    .iter()
                    .map(|option| {
                        let label =
                            CodexServerRequest::plain_required_string(option, "label")?.to_owned();
                        CodexServerRequest::plain_required_string(option, "description")?;
                        Ok(label)
                    })
                    .collect::<Result<Vec<_>, KitError>>()?,
            ),
            Some(_) => {
                return Err(KitError::protocol("input question options are malformed"));
            }
        };
        Ok(Self {
            id,
            header,
            question,
            is_other,
            is_secret,
            options,
        })
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn header(&self) -> &str {
        &self.header
    }

    pub(crate) fn question(&self) -> &str {
        &self.question
    }

    pub(crate) fn allows_other(&self) -> bool {
        self.is_other
    }

    pub(crate) fn is_secret(&self) -> bool {
        self.is_secret
    }

    pub(crate) fn options(&self) -> Option<&[String]> {
        self.options.as_deref()
    }
}
