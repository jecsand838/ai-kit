use serde::Serialize;
use serde_json::Value;

use crate::SdkError;
use crate::error::NativeErrorDetails;
use crate::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, RemoteFailure, ResponseDetails, ResponseViolation,
};

use crate::core::{
    GenerateResponseOptions, HostedJsonCursor, NormalizedGenerationParts, artifact,
    ensure_nonempty_generation_parts, file_reference, generate_response,
    push_provider_thinking_content, push_text_part, tool_call,
};
use crate::generation::{
    Artifact, Citation, FinishReason, GenerateResponse, GenerationContinuation, Usage,
};
use crate::hosted::{
    HostedCodeExecutionResult, HostedExecutableCode, HostedToolOutput, HostedToolStatus,
    HostedWebSearchAction, HostedWebSearchResult,
};
use crate::models::ModelId;
use crate::transport::CheckedRequestSpec;

use super::super::wire::{
    AnthropicCitation, AnthropicContentBlock, AnthropicMessageResponse, AnthropicUsage,
};

struct AnthropicWebSearchContent {
    results: Vec<HostedWebSearchResult>,
    status: HostedToolStatus,
}

struct AnthropicWebFetchContent {
    citations: Vec<Citation>,
    retrieved_url: Option<String>,
    status: HostedToolStatus,
}

pub(crate) fn normalize_generate_response(
    selected_model: Option<&ModelId>,
    raw: &Value,
) -> Result<GenerateResponse, SdkError> {
    let decoded =
        serde_json::from_value::<AnthropicMessageResponse>(raw.clone()).map_err(|err| {
            SdkError::invalid_response(ResponseViolation::InvalidField { field: "response" })
                .with_response_details(ResponseDetails::json(&err))
                .with_message(format!(
                    "failed to decode Anthropic message response: {err}"
                ))
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
                .at_phase(ErrorPhase::Decode)
        })?;
    if decoded.id.trim().is_empty() {
        return Err(
            SdkError::invalid_response(ResponseViolation::InvalidField { field: "id" })
                .with_message("Anthropic response did not include a valid message id".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
        );
    }
    if decoded.kind != "message" {
        return Err(
            SdkError::invalid_response(ResponseViolation::InvalidField { field: "type" })
                .with_message(format!(
                    "Anthropic response used impossible top-level type `{}`",
                    decoded.kind
                ))
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
        );
    }
    if decoded.role != "assistant" {
        return Err(
            SdkError::invalid_response(ResponseViolation::InvalidField { field: "role" })
                .with_message(format!(
                    "Anthropic response used impossible role `{}`",
                    decoded.role
                ))
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
        );
    }
    let stop_reason = decoded.stop_reason.as_deref().ok_or_else(|| {
        SdkError::invalid_response(ResponseViolation::MissingField {
            field: "stop_reason",
        })
        .with_message("Anthropic response did not include a terminal stop_reason".to_string())
        .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
    })?;
    let model = ModelId::new(decoded.model).map_err(|_| {
        SdkError::invalid_response(ResponseViolation::InvalidField { field: "model" })
            .with_message("Anthropic response contained an invalid model id".to_string())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
    })?;
    let refusal = match raw.get("stop_details") {
        None | Some(Value::Null) => None,
        Some(details) => {
            if details.get("type").and_then(Value::as_str) != Some("refusal") {
                return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                    field: "stop_details.type",
                })
                .with_message("Anthropic stop_details used an impossible type".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(details)));
            }
            let explanation = match details.get("explanation") {
                None | Some(Value::Null) => None,
                Some(Value::String(value)) => Some(value.clone()),
                Some(value) => {
                    return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                        field: "stop_details.refusal",
                    })
                    .with_message("Anthropic refusal explanation was not text".to_string())
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(value)));
                }
            };
            Some(explanation)
        }
    };
    if stop_reason == "refusal" && refusal.is_none() {
        return Err(SdkError::invalid_response(ResponseViolation::MissingField {
            field: "stop_details",
        })
        .with_message("Anthropic refusal did not include stop_details".to_string())
        .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)));
    }
    let mut portable = blocks_to_portable(&decoded.content)?;
    let finish_reason = if refusal.is_some() {
        FinishReason::ContentFilter
    } else {
        stop_reason_to_finish_reason(stop_reason)?
    };
    if let Some(Some(explanation)) = refusal
        && portable.content.is_empty()
    {
        push_text_part(&mut portable.content, Some(explanation));
    }
    if finish_reason != FinishReason::ContentFilter {
        ensure_nonempty_generation_parts(&portable, CheckedRequestSpec::diagnostic_json(raw))?;
    }
    let content_blocks = raw
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut response = generate_response(
        model.clone(),
        portable,
        GenerateResponseOptions {
            finish_reason: Some(finish_reason),
            usage: usage_from_anthropic(decoded.usage.as_ref()),
        },
    );
    response.message = response
        .message
        .with_continuation(GenerationContinuation::anthropic(
            selected_model.cloned().unwrap_or(model),
            content_blocks,
        ))?;
    Ok(response)
}

pub(crate) fn usage_from_anthropic(usage: Option<&AnthropicUsage>) -> Usage {
    let Some(usage) = usage else {
        return Usage::default();
    };
    Usage::reported_tokens(usage.input_tokens, usage.output_tokens)
        .with_cached_input(usage.cache_read_input_tokens)
        .with_reasoning(
            usage
                .output_tokens_details
                .as_ref()
                .and_then(|details| details.thinking_tokens),
        )
}

pub(crate) fn stop_reason_to_finish_reason(reason: &str) -> Result<FinishReason, SdkError> {
    match reason {
        "end_turn" | "stop_sequence" => Ok(FinishReason::Stop),
        "max_tokens" | "model_context_window_exceeded" => Ok(FinishReason::Length),
        "tool_use" => Ok(FinishReason::ToolCall),
        "refusal" => Ok(FinishReason::ContentFilter),
        "pause_turn" => Ok(FinishReason::Paused),
        other => Err(
            SdkError::invalid_response(ResponseViolation::UnknownVariant {
                field: "stop_reason",
            })
            .with_message(format!(
                "Anthropic response used unknown stop reason `{other}`"
            ))
            .with_diagnostic(None),
        ),
    }
}

pub(crate) fn blocks_to_portable(
    blocks: &[AnthropicContentBlock],
) -> Result<NormalizedGenerationParts, SdkError> {
    let mut parts = NormalizedGenerationParts::default();
    let mut hosted = AnthropicHostedOperations::new();
    for block in blocks {
        if let Some(server_tool) = AnthropicServerToolBlock::from_content_block(block) {
            hosted.observe(server_tool)?;
            continue;
        }
        match block.kind.as_str() {
            "text" => {
                push_text_part(&mut parts.content, block.text.clone());
                parts.extend_citations(
                    block
                        .citations
                        .iter()
                        .flatten()
                        .map(|raw| {
                            AnthropicCitation::decode(raw)
                                .and_then(AnthropicCitation::into_portable)
                                .map_err(|error| {
                                    error.with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            "thinking" => {
                push_provider_thinking_content(&mut parts.content, block.thinking.clone());
            }
            "redacted_thinking" => push_provider_thinking_content(&mut parts.content, None),
            "tool_use" => {
                let input = block.input.clone().ok_or_else(|| {
                    SdkError::invalid_response(ResponseViolation::MissingField { field: "input" })
                        .with_message("Anthropic tool_use block did not include input".to_string())
                        .with_diagnostic(None)
                })?;
                parts
                    .function_calls
                    .push(tool_call(block.id.clone(), block.name.clone(), input)?);
            }
            "tool_result" => {
                return Err(
                    SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                        rule: "assistant output cannot contain tool_result",
                    })
                    .with_message(
                        "Anthropic model output unexpectedly contained a tool_result block"
                            .to_string(),
                    )
                    .with_diagnostic(Some(anthropic_body("Anthropic tool_result block", block)?)),
                );
            }
            "document" | "file" => {
                parts.artifacts.push(block_artifact(block)?);
            }
            other => return Err(unsupported_content_block(other, block)?),
        }
    }
    hosted.finish(&mut parts)?;
    Ok(parts)
}

fn block_artifact(block: &AnthropicContentBlock) -> Result<Artifact, SdkError> {
    let reference = match &block.file_id {
        Some(file_id) => Some(file_reference(
            Some(file_id.clone()),
            None,
            block.media_type.clone(),
            None,
        )?),
        None => None,
    };
    artifact(
        block.media_type.clone(),
        block.title.clone(),
        None,
        block.data.clone(),
        reference,
    )
}

struct AnthropicHostedOperations<'a> {
    operations: Vec<AnthropicHostedOperation<'a>>,
}

enum AnthropicHostedOperation<'a> {
    Invocation {
        id: &'a str,
        block: &'a AnthropicContentBlock,
    },
    Result {
        id: &'a str,
        block: AnthropicServerToolBlock<'a>,
    },
    Paired {
        id: &'a str,
        invocation: &'a AnthropicContentBlock,
        result: AnthropicServerToolBlock<'a>,
    },
}

impl<'a> AnthropicHostedOperation<'a> {
    fn id(&self) -> &'a str {
        match self {
            Self::Invocation { id, .. } | Self::Result { id, .. } | Self::Paired { id, .. } => id,
        }
    }

    fn attach(&mut self, tool: AnthropicServerToolBlock<'a>) -> Result<(), SdkError> {
        let (invocation, result) = match (&*self, tool) {
            (Self::Invocation { block, .. }, result)
                if !matches!(result, AnthropicServerToolBlock::Invocation(_)) =>
            {
                (*block, result)
            }
            (Self::Result { block, .. }, AnthropicServerToolBlock::Invocation(invocation)) => {
                (invocation, *block)
            }
            _ => return Err(tool.invalid_link("duplicate hosted invocation or result ID")),
        };
        if AnthropicServerToolBlock::Invocation(invocation).family()? != result.family()? {
            return Err(tool.invalid_link("hosted result ID refers to a different tool family"));
        }
        *self = Self::Paired {
            id: self.id(),
            invocation,
            result,
        };
        Ok(())
    }

    fn into_output(self) -> Result<HostedToolOutput, SdkError> {
        let (invocation, result) = match self {
            Self::Invocation { block, .. } => {
                return AnthropicServerToolBlock::Invocation(block).into_hosted_output();
            }
            Self::Result { block, .. } => return block.into_hosted_output(),
            Self::Paired {
                invocation, result, ..
            } => (invocation, result),
        };
        let mut output = result.into_hosted_output()?;
        let source = AnthropicServerToolBlock::Invocation(invocation);
        match (&mut output, source.into_hosted_output()?) {
            (
                HostedToolOutput::WebSearch { action, .. },
                HostedToolOutput::WebSearch { action: native, .. },
            ) => *action = native,
            (
                HostedToolOutput::UrlContext { requested_url, .. },
                HostedToolOutput::UrlContext {
                    requested_url: native,
                    ..
                },
            ) => *requested_url = native,
            (
                HostedToolOutput::CodeExecution { code, .. },
                HostedToolOutput::CodeExecution {
                    code: mut native, ..
                },
            ) => {
                native.append(code);
                *code = native;
            }
            _ => {
                return Err(source
                    .invalid_link("correlated native tool families changed during normalization"));
            }
        }
        Ok(output)
    }
}

impl<'a> AnthropicHostedOperations<'a> {
    fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    fn observe(&mut self, tool: AnthropicServerToolBlock<'a>) -> Result<(), SdkError> {
        let block = tool.block();
        let invocation = matches!(tool, AnthropicServerToolBlock::Invocation(_));
        let id = if invocation {
            block.id.as_deref()
        } else {
            block.tool_use_id.as_deref()
        }
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| tool.invalid_link("hosted block requires an exact native ID"))?;
        if let Some(operation) = self
            .operations
            .iter_mut()
            .find(|operation| operation.id() == id)
        {
            operation.attach(tool)?;
        } else if invocation {
            self.operations
                .push(AnthropicHostedOperation::Invocation { id, block });
        } else {
            self.operations
                .push(AnthropicHostedOperation::Result { id, block: tool });
        }
        Ok(())
    }

    fn finish(self, parts: &mut NormalizedGenerationParts) -> Result<(), SdkError> {
        for operation in self.operations {
            parts.push_hosted_output(operation.into_output()?);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum AnthropicServerToolBlock<'a> {
    Invocation(&'a AnthropicContentBlock),
    WebSearchResult(&'a AnthropicContentBlock),
    WebFetchResult(&'a AnthropicContentBlock),
    CodeExecutionResult(&'a AnthropicContentBlock),
}

impl<'a> AnthropicServerToolBlock<'a> {
    fn from_content_block(block: &'a AnthropicContentBlock) -> Option<Self> {
        match block.kind.as_str() {
            "server_tool_use" => Some(Self::Invocation(block)),
            "web_search_tool_result" | "web_search_tool_result_error" => {
                Some(Self::WebSearchResult(block))
            }
            "web_fetch_tool_result" | "web_fetch_tool_result_error" => {
                Some(Self::WebFetchResult(block))
            }
            "code_execution_tool_result"
            | "code_execution_tool_result_error"
            | "bash_code_execution_tool_result"
            | "bash_code_execution_tool_result_error"
            | "text_editor_code_execution_tool_result"
            | "text_editor_code_execution_tool_result_error" => {
                Some(Self::CodeExecutionResult(block))
            }
            _ => None,
        }
    }

    fn block(&self) -> &'a AnthropicContentBlock {
        match self {
            Self::Invocation(block)
            | Self::WebSearchResult(block)
            | Self::WebFetchResult(block)
            | Self::CodeExecutionResult(block) => block,
        }
    }

    fn family(&self) -> Result<&str, SdkError> {
        Ok(match self {
            Self::Invocation(block) => {
                match block.name.as_deref() {
                    Some(
                        name @ ("web_search"
                        | "web_fetch"
                        | "code_execution"
                        | "bash_code_execution"
                        | "text_editor_code_execution"),
                    ) => name,
                    Some(name) => {
                        return Err(SdkError::invalid_response(
                            ResponseViolation::UnknownVariant { field: "name" },
                        )
                        .at_phase(ErrorPhase::Normalize)
                        .with_response_details(ResponseDetails::Discriminator {
                            actual: name.to_owned(),
                        })
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(block)));
                    }
                    None => {
                        return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                            field: "name",
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_response_details(ResponseDetails::FieldType {
                            expected: crate::error::JsonValueKind::String,
                            actual: crate::error::JsonValueKind::Absent,
                        })
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(block)));
                    }
                }
            }
            Self::WebSearchResult(_) => "web_search",
            Self::WebFetchResult(_) => "web_fetch",
            Self::CodeExecutionResult(block) if block.kind.starts_with("bash_") => {
                "bash_code_execution"
            }
            Self::CodeExecutionResult(block) if block.kind.starts_with("text_editor_") => {
                "text_editor_code_execution"
            }
            Self::CodeExecutionResult(_) => "code_execution",
        })
    }

    fn invalid_link(&self, rule: &'static str) -> SdkError {
        SdkError::invalid_response(ResponseViolation::ProtocolViolation { rule })
            .at_phase(ErrorPhase::Normalize)
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(self.block()))
    }

    fn into_hosted_output(self) -> Result<HostedToolOutput, SdkError> {
        match self {
            Self::Invocation(block) => self.invocation(block),
            Self::WebSearchResult(block) => {
                let content = AnthropicServerToolResult::new(block).web_search_content()?;
                Ok(HostedToolOutput::WebSearch {
                    status: Some(content.status),
                    action: None,
                    results: content.results,
                    citations: Vec::new(),
                })
            }
            Self::WebFetchResult(block) => {
                let content = AnthropicServerToolResult::new(block).web_fetch_content()?;
                Ok(HostedToolOutput::UrlContext {
                    requested_url: None,
                    retrieved_url: content.retrieved_url,
                    status: Some(content.status),
                    citations: content.citations,
                })
            }
            Self::CodeExecutionResult(block) => {
                let mut code = Vec::new();
                let mut results = Vec::new();
                let mut artifacts = Vec::new();
                let result = AnthropicServerToolResult::new(block);
                let status = result.code_status()?;
                result.collect_code_content(&mut code, &mut results, &mut artifacts)?;
                if block.file_id.is_some() || block.data.is_some() {
                    artifacts.push(block_artifact(block)?);
                }
                Ok(HostedToolOutput::CodeExecution {
                    status: Some(status),
                    code,
                    results,
                    artifacts,
                })
            }
        }
    }

    fn invocation(&self, block: &AnthropicContentBlock) -> Result<HostedToolOutput, SdkError> {
        let body = anthropic_body("Anthropic hosted invocation", block)?;
        let input = block
            .input
            .as_ref()
            .ok_or_else(|| self.invalid_link("hosted invocation requires input"))?;
        let cursor = HostedJsonCursor::new("Anthropic hosted invocation input", body, input);
        match self.family()? {
            "web_search" => Ok(HostedToolOutput::WebSearch {
                status: Some(HostedToolStatus::InProgress),
                action: Some(HostedWebSearchAction::Search {
                    query: Some(cursor.required_string_field("query")?),
                    queries: Vec::new(),
                    sources: Vec::new(),
                }),
                results: Vec::new(),
                citations: Vec::new(),
            }),
            "web_fetch" => Ok(HostedToolOutput::UrlContext {
                requested_url: Some(cursor.required_string_field("url")?),
                retrieved_url: None,
                status: Some(HostedToolStatus::InProgress),
                citations: Vec::new(),
            }),
            _ => {
                let code = cursor
                    .optional_str_field("code")?
                    .or(cursor.optional_str_field("command")?)
                    .map(|code| HostedExecutableCode {
                        language: None,
                        code,
                    })
                    .into_iter()
                    .collect();
                Ok(HostedToolOutput::CodeExecution {
                    status: Some(HostedToolStatus::InProgress),
                    code,
                    results: Vec::new(),
                    artifacts: Vec::new(),
                })
            }
        }
    }
}

struct AnthropicServerToolResult<'a> {
    block: &'a AnthropicContentBlock,
}

impl<'a> AnthropicServerToolResult<'a> {
    fn new(block: &'a AnthropicContentBlock) -> Self {
        Self { block }
    }

    fn body(&self) -> Result<NativeErrorDetails, SdkError> {
        anthropic_body("Anthropic content block", self.block)
    }

    fn content_objects(
        &self,
        context: &str,
    ) -> Result<Vec<&'a serde_json::Map<String, Value>>, SdkError> {
        let body = self.body()?;
        let content = self.block.content.as_ref().ok_or_else(|| {
            SdkError::invalid_response(ResponseViolation::MissingField { field: "content" })
                .with_message(format!("{context} did not include content"))
                .with_diagnostic(Some(body.clone()))
        })?;
        HostedJsonCursor::new(context, body, content).objects_or_single()
    }

    fn content_type(
        &self,
        object: &'a serde_json::Map<String, Value>,
        context: &str,
    ) -> Result<&'a str, SdkError> {
        HostedJsonCursor::for_borrowed_object(context, object).required_str_field("type")
    }

    fn optional_string(
        &self,
        object: &'a serde_json::Map<String, Value>,
        field: &'static str,
        context: &str,
    ) -> Result<Option<String>, SdkError> {
        HostedJsonCursor::for_borrowed_object(context, object).optional_str_field(field)
    }

    fn optional_i64(
        &self,
        object: &'a serde_json::Map<String, Value>,
        field: &'static str,
        context: &str,
    ) -> Result<Option<i64>, SdkError> {
        HostedJsonCursor::for_borrowed_object(context, object).optional_i64_field(field)
    }

    fn web_search_content(&self) -> Result<AnthropicWebSearchContent, SdkError> {
        let mut results = Vec::new();
        let mut failure = None;
        for object in self.content_objects("Anthropic web search result")? {
            match self.content_type(object, "Anthropic web search result")? {
                "web_search_result" => {
                    let url = self
                        .optional_string(object, "url", "Anthropic web search result")?
                        .ok_or_else(|| {
                            SdkError::invalid_response(ResponseViolation::MissingField {
                                field: "url",
                            })
                            .with_message(
                                "Anthropic web search result did not include a URL".to_string(),
                            )
                            .with_diagnostic(
                                CheckedRequestSpec::diagnostic_json(&Value::Object(object.clone())),
                            )
                        })?;
                    results.push(HostedWebSearchResult {
                        title: self.optional_string(
                            object,
                            "title",
                            "Anthropic web search result",
                        )?,
                        url,
                        snippet: None,
                    });
                }
                "web_search_tool_result_error" => {
                    failure = Some(self.failure(object)?);
                }
                other => return Err(self.unknown_content(other, object)),
            }
        }
        let status = failure.unwrap_or(HostedToolStatus::Completed);
        Ok(AnthropicWebSearchContent { results, status })
    }

    fn web_fetch_content(&self) -> Result<AnthropicWebFetchContent, SdkError> {
        let objects = self.content_objects("Anthropic web fetch result")?;
        if objects.len() != 1 {
            return Err(
                SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "web fetch must contain one native result",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_diagnostic(Some(self.body()?)),
            );
        }
        let object = objects[0];
        match self.content_type(object, "Anthropic web fetch result")? {
            "web_fetch_result" => {
                let citation = self.web_fetch_citation(object)?;
                let retrieved_url =
                    self.optional_string(object, "url", "Anthropic web fetch result")?;
                Ok(AnthropicWebFetchContent {
                    retrieved_url,
                    citations: vec![citation],
                    status: HostedToolStatus::Completed,
                })
            }
            "web_fetch_tool_result_error" => Ok(AnthropicWebFetchContent {
                retrieved_url: None,
                citations: Vec::new(),
                status: self.failure(object)?,
            }),
            other => Err(self.unknown_content(other, object)),
        }
    }

    fn failure(
        &self,
        object: &'a serde_json::Map<String, Value>,
    ) -> Result<HostedToolStatus, SdkError> {
        let cursor =
            HostedJsonCursor::for_object("Anthropic hosted tool error", self.body()?, object);
        let code = cursor.required_string_field("error_code")?;
        let (kind, basis) = match code.as_str() {
            "url_not_accessible" => (ErrorKind::ProviderRejected, ClassificationBasis::NativeCode),
            "invalid_tool_input" | "unsupported_content_type" => {
                (ErrorKind::InvalidRequest, ClassificationBasis::NativeCode)
            }
            "url_not_allowed" | "url_not_in_prior_context" => {
                (ErrorKind::PermissionDenied, ClassificationBasis::NativeCode)
            }
            "too_many_requests" => (ErrorKind::RateLimited, ClassificationBasis::NativeCode),
            "max_uses_exceeded" => (
                ErrorKind::ResourceExhausted,
                ClassificationBasis::NativeCode,
            ),
            "unavailable" => (
                ErrorKind::ServiceUnavailable,
                ClassificationBasis::NativeCode,
            ),
            "url_too_long" | "request_too_large" | "query_too_long" => {
                (ErrorKind::RequestTooLarge, ClassificationBasis::NativeCode)
            }
            _ => (
                ErrorKind::ProviderRejected,
                ClassificationBasis::Unclassified,
            ),
        };
        let mut details = self.body()?;
        details.code = Some(code);
        details.error_type = Some(cursor.required_string_field("type")?);
        details.tool_call_id = self.block.tool_use_id.clone();
        Ok(HostedToolStatus::Failed {
            error: RemoteFailure::from_native(kind, basis, details)?,
        })
    }

    fn process_failure(&self, exit_code: i64) -> Result<HostedToolStatus, SdkError> {
        let mut details = self.body()?;
        details.execution_exit_code = Some(exit_code);
        details.tool_call_id = self.block.tool_use_id.clone();
        Ok(HostedToolStatus::Failed {
            error: RemoteFailure::from_native(
                ErrorKind::ProviderRejected,
                ClassificationBasis::NativeOutcome,
                details,
            )?,
        })
    }

    fn web_fetch_citation(
        &self,
        object: &'a serde_json::Map<String, Value>,
    ) -> Result<Citation, SdkError> {
        let title = match object.get("content").and_then(Value::as_object) {
            Some(content) => {
                self.optional_string(content, "title", "Anthropic web fetch content")?
            }
            None => None,
        };
        let url = self
            .optional_string(object, "url", "Anthropic web fetch result")?
            .ok_or_else(|| {
                SdkError::invalid_response(ResponseViolation::MissingField { field: "url" })
                    .with_message("Anthropic web fetch result did not include a URL".to_string())
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(&Value::Object(
                        object.clone(),
                    )))
            })?;
        Ok(Citation::Url {
            title,
            url,
            snippet: None,
            start_index: None,
            end_index: None,
        })
    }

    fn collect_code_content(
        &self,
        code: &mut Vec<HostedExecutableCode>,
        results: &mut Vec<HostedCodeExecutionResult>,
        artifacts: &mut Vec<Artifact>,
    ) -> Result<(), SdkError> {
        for object in self.content_objects("Anthropic code execution result")? {
            match self.content_type(object, "Anthropic code execution result")? {
                "code_execution_result"
                | "bash_code_execution_result"
                | "text_editor_code_execution_result"
                | "encrypted_code_execution_result" => {
                    self.collect_code_result_object(object, code, results, artifacts)?;
                }
                "code_execution_tool_result_error"
                | "bash_code_execution_tool_result_error"
                | "text_editor_code_execution_tool_result_error" => {}
                other => return Err(self.unknown_content(other, object)),
            }
        }
        Ok(())
    }

    fn collect_code_result_object(
        &self,
        object: &'a serde_json::Map<String, Value>,
        code: &mut Vec<HostedExecutableCode>,
        results: &mut Vec<HostedCodeExecutionResult>,
        artifacts: &mut Vec<Artifact>,
    ) -> Result<(), SdkError> {
        if let Some(code_text) =
            self.optional_string(object, "code", "Anthropic code execution result")?
        {
            code.push(HostedExecutableCode {
                language: object
                    .get("language")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                code: code_text,
            });
        }
        if object.contains_key("stdout") || object.contains_key("stderr") {
            let return_code =
                self.optional_i64(object, "return_code", "Anthropic code execution result")?;
            results.push(HostedCodeExecutionResult {
                status: match return_code {
                    Some(code) if code != 0 => Some(self.process_failure(code)?),
                    Some(_) => Some(HostedToolStatus::Completed),
                    None => None,
                },
                logs: None,
                stdout: self.optional_string(
                    object,
                    "stdout",
                    "Anthropic code execution result",
                )?,
                stderr: self.optional_string(
                    object,
                    "stderr",
                    "Anthropic code execution result",
                )?,
                exit_code: return_code,
            });
        }
        match object.get("content") {
            Some(Value::Array(items)) => {
                for item in items {
                    let object = item.as_object().ok_or_else(|| {
                        SdkError::invalid_response(ResponseViolation::InvalidField {
                            field: "content",
                        })
                        .with_message("Anthropic code artifact must be an object".to_string())
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(item))
                    })?;
                    self.collect_code_artifact(object, artifacts)?;
                }
            }
            Some(Value::Object(content)) => self.collect_code_artifact(content, artifacts)?,
            Some(Value::Null) | None => self.collect_code_artifact(object, artifacts)?,
            Some(other) => {
                return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                    field: "content",
                })
                .with_message(
                    "Anthropic code result content must be an object or array".to_string(),
                )
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(other)));
            }
        }
        Ok(())
    }

    fn collect_code_artifact(
        &self,
        object: &serde_json::Map<String, Value>,
        artifacts: &mut Vec<Artifact>,
    ) -> Result<(), SdkError> {
        let Some(file_id) = self.optional_string(object, "file_id", "Anthropic code artifact")?
        else {
            return Ok(());
        };
        let media_type = self.optional_string(object, "mime_type", "Anthropic code artifact")?;
        let file_name = self
            .optional_string(object, "filename", "Anthropic code artifact")?
            .or(self.optional_string(object, "title", "Anthropic code artifact")?);
        let data = self.optional_string(object, "data", "Anthropic code artifact")?;
        artifacts.push(artifact(
            media_type.clone(),
            file_name,
            None,
            data,
            Some(file_reference(Some(file_id), None, media_type, None)?),
        )?);
        Ok(())
    }

    fn code_status(&self) -> Result<HostedToolStatus, SdkError> {
        let objects = self.content_objects("Anthropic code execution result")?;
        if objects.is_empty() {
            return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                field: "content",
            })
            .with_diagnostic(Some(self.body()?)));
        }
        for object in objects {
            let kind = self.content_type(object, "Anthropic code execution result")?;
            if matches!(kind, "code_execution_result" | "bash_code_execution_result") {
                let cursor =
                    HostedJsonCursor::for_object("Anthropic process result", self.body()?, object);
                cursor.required_str_field("stdout")?;
                cursor.required_str_field("stderr")?;
                cursor.optional_i64_field("return_code")?.ok_or_else(|| {
                    cursor.invalid(
                        ResponseViolation::MissingField {
                            field: "return_code",
                        },
                        "process result did not include return_code",
                    )
                })?;
            }
            if matches!(
                self.content_type(object, "Anthropic code execution result")?,
                "code_execution_tool_result_error"
                    | "bash_code_execution_tool_result_error"
                    | "text_editor_code_execution_tool_result_error"
            ) {
                return self.failure(object);
            }
            if let Some(code) =
                self.optional_i64(object, "return_code", "Anthropic code execution result")?
                && code != 0
            {
                return self.process_failure(code);
            }
        }
        Ok(HostedToolStatus::Completed)
    }

    fn unknown_content(&self, kind: &str, object: &serde_json::Map<String, Value>) -> SdkError {
        SdkError::invalid_response(ResponseViolation::UnknownVariant {
            field: "content.type",
        })
        .at_phase(ErrorPhase::Normalize)
        .with_response_details(ResponseDetails::Discriminator {
            actual: kind.to_owned(),
        })
        .with_diagnostic(CheckedRequestSpec::diagnostic_json(&Value::Object(
            object.clone(),
        )))
    }
}

fn unsupported_content_block(
    kind: &str,
    block: &AnthropicContentBlock,
) -> Result<SdkError, SdkError> {
    Ok(
        SdkError::invalid_response(ResponseViolation::UnknownVariant { field: "type" })
            .with_message(format!(
                "Anthropic content block type `{kind}` is not supported"
            ))
            .with_diagnostic(Some(anthropic_body("Anthropic content block", block)?)),
    )
}

fn anthropic_body<T: Serialize>(
    target: &'static str,
    value: &T,
) -> Result<NativeErrorDetails, SdkError> {
    CheckedRequestSpec::diagnostic_json(value).ok_or_else(|| {
        SdkError::invalid_response(ResponseViolation::ProtocolViolation {
            rule: "diagnostic encoding failed",
        })
        .with_message(format!("failed to encode {target} for diagnostics"))
        .with_diagnostic(None)
    })
}
