use std::collections::BTreeSet;

use reqwest::Method;
use serde_json::Value;

use crate::SdkError;
use crate::core::{
    FrameDecoder, StreamFrame, StreamingAccumulator, push_provider_thinking_content,
};
use crate::error::{
    ErrorKind, ErrorPhase, JsonValueKind, Operation, ResponseDetails, ResponseViolation,
};
use crate::generation::{
    ContentPart, FinishReason, GenerateEvent, GenerateEventStream, GenerateResponse,
    GenerationContinuation, Message,
};
use crate::models::ModelId;
use crate::transport::{CheckedRequestSpec, SseEvent};

use super::api::{GEMINI_REQUESTS, GeminiApiContext};
use super::generate_content::{function_call_to_tool_call, usage_from_metadata};
use super::wire::{
    GeminiCandidate, GeminiErrorObject, GeminiGenerateContentResponse, GeminiGroundingMetadata,
    GeminiUrlContextMetadata,
};

#[derive(Clone, Debug)]
pub(crate) struct GeminiStreamState {
    shared: StreamingAccumulator,
    selected_model: ModelId,
    native_model_version: Option<String>,
    seen_tool_ids: BTreeSet<String>,
    thinking: String,
    raw_parts: Vec<Value>,
    grounding_metadata: Option<GeminiGroundingMetadata>,
    url_context_metadata: Option<GeminiUrlContextMetadata>,
    pending_completion: Option<GeminiTerminal>,
    saw_candidate: bool,
}

#[derive(Clone, Debug)]
enum GeminiTerminal {
    PromptBlocked(SseEvent),
    Candidate {
        event: SseEvent,
        reason: FinishReason,
    },
}

impl GeminiStreamState {
    pub(crate) fn new(selected_model: ModelId) -> Self {
        Self {
            shared: StreamingAccumulator::with_model(selected_model.clone()),
            selected_model,
            native_model_version: None,
            seen_tool_ids: BTreeSet::new(),
            thinking: String::new(),
            raw_parts: Vec::new(),
            grounding_metadata: None,
            url_context_metadata: None,
            pending_completion: None,
            saw_candidate: false,
        }
    }

    fn invalid(reason: ResponseViolation, message: impl Into<String>) -> SdkError {
        SdkError::new(
            ErrorKind::InvalidResponse(reason),
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_message(message)
    }

    fn invalid_body(reason: ResponseViolation, message: impl Into<String>, body: &str) -> SdkError {
        Self::invalid(reason, message)
            .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(body.as_bytes())))
    }

    fn push_thinking(
        &mut self,
        delta: impl Into<String>,
    ) -> Result<Option<GenerateEvent>, SdkError> {
        let delta = delta.into();
        if delta.is_empty() {
            return Ok(None);
        }
        self.shared.retain(delta.len(), 0)?;
        self.thinking.push_str(&delta);
        Ok(Some(GenerateEvent::ThinkingDelta { delta }))
    }
}

pub(crate) async fn start_generate_stream(
    api: GeminiApiContext,
    model: ModelId,
    body: Value,
) -> Result<GenerateEventStream, SdkError> {
    let path = format!("v1beta/{model}:streamGenerateContent?alt=sse");
    let execution = api.transport().execution();
    let timeout = execution.timeout();
    let response = GEMINI_REQUESTS
        .response(
            &path,
            api.request(Method::POST, &path)?.json(&body),
            execution,
        )
        .await?;
    let mut accumulator = GeminiStreamState::new(model);
    FrameDecoder::sse().into_stream(
        response,
        "gemini generation stream",
        timeout,
        move |frame| match frame {
            StreamFrame::Sse(event) => accumulator.normalize(event),
            StreamFrame::End => accumulator.finish(),
            StreamFrame::Line(_) => Err(GeminiStreamState::invalid(
                ResponseViolation::ProtocolViolation {
                    rule: "expected SSE frame",
                },
                "Gemini streaming received a non-SSE frame",
            )),
        },
    )
}

impl GeminiStreamState {
    fn normalize(&mut self, event: SseEvent) -> Result<Vec<GenerateEvent>, SdkError> {
        if event.data.trim().is_empty() || event.data.trim() == "[DONE]" {
            return Ok(Vec::new());
        }
        let payload = serde_json::from_str::<Value>(&event.data).map_err(|err| {
            GeminiStreamState::invalid_body(
                ResponseViolation::MalformedJson,
                format!("failed to decode Gemini SSE payload: {err}"),
                &event.data,
            )
            .with_response_details(ResponseDetails::json(&err))
            .at_phase(ErrorPhase::Decode)
        })?;
        if let Some(error) = payload.get("error") {
            let error: GeminiErrorObject =
                serde_json::from_value(error.clone()).map_err(|error| {
                    GeminiStreamState::invalid_body(
                        ResponseViolation::InvalidField { field: "error" },
                        format!("failed to decode Gemini stream error: {error}"),
                        &event.data,
                    )
                    .with_response_details(ResponseDetails::json(&error))
                    .at_phase(ErrorPhase::Decode)
                })?;
            let mut diagnostic = CheckedRequestSpec::diagnostic_body(event.data.as_bytes());
            diagnostic.event_id = event.id.clone();
            let failure = error.remote_failure(Some(diagnostic))?;
            return Err(SdkError::from_remote(
                failure,
                Operation::GenerateStream,
                ErrorPhase::Stream,
            ));
        }
        let parsed = serde_json::from_value::<GeminiGenerateContentResponse>(payload.clone())
            .map_err(|err| {
                GeminiStreamState::invalid_body(
                    ResponseViolation::InvalidField { field: "response" },
                    format!("failed to decode Gemini stream response chunk: {err}"),
                    &event.data,
                )
                .with_response_details(ResponseDetails::json(&err))
                .at_phase(ErrorPhase::Decode)
            })?;
        if parsed.candidates.len() > 1 {
            return Err(Self::invalid(
                ResponseViolation::ProtocolViolation {
                    rule: "response must contain exactly one candidate",
                },
                "Gemini stream returned multiple candidates",
            ));
        }
        let blocked = parsed.prompt_blocked(&payload).map_err(|error| {
            error
                .at_operation(Operation::GenerateStream)
                .at_phase(ErrorPhase::Stream)
        })?;
        if (blocked && self.saw_candidate)
            || (!parsed.candidates.is_empty()
                && matches!(
                    self.pending_completion,
                    Some(GeminiTerminal::PromptBlocked(_))
                ))
        {
            return Err(Self::invalid(
                ResponseViolation::ProtocolViolation {
                    rule: "prompt block must not coexist with candidate output",
                },
                "Gemini prompt block and candidate output contradict each other",
            ));
        }
        let mut finish_reason = None;
        if let Some(candidate) = parsed.candidates.first() {
            self.saw_candidate = true;
            finish_reason = candidate.finish_reason().map_err(|error| {
                let phase = if error.kind() == ErrorKind::Unsupported {
                    ErrorPhase::Normalize
                } else {
                    ErrorPhase::Stream
                };
                error
                    .at_operation(Operation::GenerateStream)
                    .at_phase(phase)
            })?;
            if self
                .pending_completion
                .as_ref()
                .is_some_and(|terminal| matches!(terminal, GeminiTerminal::Candidate { reason, .. } if !matches!(reason, FinishReason::Stop)))
            {
                return Err(Self::invalid(
                    ResponseViolation::ProtocolViolation {
                        rule: "Gemini stream continued after an unsuccessful finish reason",
                    },
                    "Gemini candidate continued after a non-STOP finish reason",
                ));
            }
            // A later candidate must finish independently. Candidate-free
            // usage frames do not invalidate the latest finished candidate.
            self.pending_completion = None;
            if candidate.index.is_some_and(|index| index != 0) {
                return Err(Self::invalid(
                    ResponseViolation::InvalidField {
                        field: "candidates.index",
                    },
                    "Gemini stream candidate used a nonzero index",
                ));
            }
            if let Some(content) = &candidate.content {
                content.validate_model_role().map_err(|error| {
                    error
                        .at_operation(Operation::GenerateStream)
                        .at_phase(ErrorPhase::Stream)
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(&payload))
                })?;
            }
        }
        let mut emitted = Vec::new();
        if let Some(started) = self.shared.started_event(self.selected_model.clone()) {
            emitted.push(started);
        }
        if let Some(parts) = payload
            .pointer("/candidates/0/content/parts")
            .and_then(Value::as_array)
        {
            for part in parts {
                self.shared.retain(part.to_string().len(), 1)?;
                self.raw_parts.push(part.clone());
            }
        }
        if let Some(version) = parsed.model_version.as_deref() {
            if version.is_empty()
                || version.len() > 256
                || version.trim() != version
                || version.chars().any(char::is_control)
            {
                return Err(GeminiStreamState::invalid(
                    ResponseViolation::InvalidField {
                        field: "modelVersion",
                    },
                    "Gemini stream used an invalid modelVersion",
                ));
            }
            if let Some(previous) = self.native_model_version.as_deref()
                && previous != version
            {
                return Err(GeminiStreamState::invalid(
                    ResponseViolation::IdentityMismatch {
                        field: "modelVersion",
                    },
                    "Gemini stream changed modelVersion between chunks",
                )
                .with_response_details(ResponseDetails::Identity {
                    expected: previous.to_owned(),
                    actual: version.to_owned(),
                }));
            }
            self.native_model_version = Some(version.to_string());
        }

        if let Some(candidate) = parsed.candidates.first() {
            self.retain_metadata(candidate, &payload)?;
            if let Some(content) = &candidate.content {
                let thinking_delta = Self::text_from_content(content, true);
                if !thinking_delta.is_empty()
                    && let Some(event) = self.push_thinking(thinking_delta)?
                {
                    emitted.push(event);
                }
                let text = Self::text_from_content(content, false);
                if !text.is_empty()
                    && let Some(event) = self.shared.push_text(&text)?
                {
                    emitted.push(event);
                }
                for part in &content.parts {
                    if let Some(function_call) = &part.function_call {
                        let tool_call = function_call_to_tool_call(function_call)?;
                        if let Some(id) = tool_call.id().map(str::to_owned) {
                            if self.seen_tool_ids.insert(id) {
                                self.shared.retain(tool_call.id().map_or(0, str::len), 0)?;
                                self.shared.push_call(tool_call)?;
                            }
                        } else {
                            self.shared.push_call(tool_call)?;
                        }
                    }
                }
            }
        }

        if finish_reason.is_some() || blocked {
            self.shared.retain(
                event
                    .data
                    .len()
                    .saturating_add(event.event.as_ref().map_or(0, String::len))
                    .saturating_add(event.id.as_ref().map_or(0, String::len)),
                1,
            )?;
            self.pending_completion = Some(match finish_reason {
                Some(reason) => GeminiTerminal::Candidate { event, reason },
                None => GeminiTerminal::PromptBlocked(event),
            });
        }

        if let Some(usage) = parsed.usage_metadata.as_ref() {
            let usage = usage_from_metadata(Some(usage));
            self.shared.set_usage(usage.clone());
            emitted.push(GenerateEvent::Usage(usage));
        }
        Ok(emitted)
    }

    fn finish(&mut self) -> Result<Vec<GenerateEvent>, SdkError> {
        let terminal = self.pending_completion.take().ok_or_else(|| {
            Self::invalid(
                ResponseViolation::UnexpectedEnd,
                "Gemini stream ended without a finished final candidate",
            )
        })?;
        let event = match terminal {
            GeminiTerminal::PromptBlocked(event) | GeminiTerminal::Candidate { event, .. } => event,
        };
        self.complete_response(&event.data)
            .map(|response| vec![GenerateEvent::Completed(Box::new(response))])
            .map_err(|error| {
                let mut details = error.native_details().clone();
                let diagnostic = CheckedRequestSpec::diagnostic_body(event.data.as_bytes());
                // Completion used accumulated parts, but its evidence remains
                // the original terminal candidate frame, never assembled JSON.
                details.body = diagnostic.body;
                details.truncated |= diagnostic.truncated;
                details.event_id = event.id.or(details.event_id);
                error
                    .at_operation(Operation::GenerateStream)
                    .with_native_capture(details)
            })
    }

    fn complete_response(&mut self, body: &str) -> Result<GenerateResponse, SdkError> {
        let original = serde_json::from_str::<Value>(body).map_err(|error| {
            Self::invalid(
                ResponseViolation::MalformedJson,
                "retained Gemini completion frame is not valid JSON",
            )
            .with_response_details(ResponseDetails::json(&error))
            .at_phase(ErrorPhase::Decode)
        })?;
        let mut parsed = serde_json::from_value::<GeminiGenerateContentResponse>(original.clone())
            .map_err(|error| {
                Self::invalid(
                    ResponseViolation::InvalidField { field: "response" },
                    "retained Gemini completion frame is malformed",
                )
                .with_response_details(ResponseDetails::json(&error))
                .at_phase(ErrorPhase::Decode)
            })?;
        let raw_parts = std::mem::take(&mut self.raw_parts);
        if !parsed.prompt_blocked(&original)? {
            let candidate = parsed.candidates.first_mut().ok_or_else(|| {
                Self::invalid(
                    ResponseViolation::MissingField {
                        field: "candidates",
                    },
                    "terminal stream omitted candidate",
                )
            })?;
            candidate.grounding_metadata = self.grounding_metadata.take();
            candidate.url_context_metadata = self.url_context_metadata.take();
            if !raw_parts.is_empty() {
                if candidate.content.is_none()
                    && !matches!(
                        candidate.finish_reason()?,
                        Some(FinishReason::ContentFilter | FinishReason::Error)
                    )
                {
                    let actual = JsonValueKind::of(original.pointer("/candidates/0/content"));
                    return Err(Self::invalid(
                        if actual == JsonValueKind::Absent {
                            ResponseViolation::MissingField {
                                field: "candidates.content",
                            }
                        } else {
                            ResponseViolation::InvalidField {
                                field: "candidates.content",
                            }
                        },
                        "Gemini terminal stream response lost candidate content",
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::Object,
                        actual,
                    }));
                }
                // A filtered or account-policy terminal may omit content, but
                // accumulated parts are actual provider output. Keep the original frame.
                candidate.content.get_or_insert_default().parts =
                    serde_json::from_value(Value::Array(raw_parts.clone())).map_err(|error| {
                        Self::invalid(
                            ResponseViolation::InvalidField {
                                field: "content.parts",
                            },
                            "retained Gemini parts are malformed",
                        )
                        .with_response_details(ResponseDetails::json(&error))
                    })?;
            }
        }
        let mut completed = parsed.into_response(&self.selected_model, &original, Vec::new())?;
        let (accumulated_text, accumulated_calls) = self.shared.take_output();
        let accumulated_thinking = std::mem::take(&mut self.thinking);
        let content = Self::completed_content(
            completed.message.content().to_vec(),
            accumulated_text,
            accumulated_thinking,
        );
        let calls = if accumulated_calls.is_empty() {
            completed.message.function_calls().to_vec()
        } else {
            accumulated_calls
        };
        completed.message = Message::returned_assistant(content, calls, None);
        if !raw_parts.is_empty() {
            completed.message =
                completed
                    .message
                    .with_continuation(GenerationContinuation::gemini(
                        self.selected_model.clone(),
                        raw_parts,
                    ))?;
        }
        completed.usage = self.shared.usage().clone();
        Ok(completed)
    }

    fn retain_metadata(
        &mut self,
        candidate: &GeminiCandidate,
        payload: &Value,
    ) -> Result<(), SdkError> {
        if let Some(grounding) = &candidate.grounding_metadata {
            let bytes = payload
                .pointer("/candidates/0/groundingMetadata")
                .map_or(0, |value| value.to_string().len());
            let items = grounding
                .grounding_chunks
                .len()
                .saturating_add(grounding.grounding_supports.len())
                .saturating_add(grounding.web_search_queries.len())
                .saturating_add(grounding.retrieval_queries.len());
            self.shared.retain(bytes, items)?;
            self.grounding_metadata
                .get_or_insert_default()
                .append(grounding.clone());
        }
        if let Some(url_context) = &candidate.url_context_metadata {
            let bytes = payload
                .pointer("/candidates/0/urlContextMetadata")
                .map_or(0, |value| value.to_string().len());
            self.shared.retain(bytes, url_context.url_metadata.len())?;
            let accumulated = self.url_context_metadata.get_or_insert_default();
            accumulated
                .url_metadata
                .extend(url_context.url_metadata.clone());
            accumulated.extra.extend(url_context.extra.clone());
        }
        Ok(())
    }

    fn text_from_content(content: &super::wire::GeminiContent, thought: bool) -> String {
        content
            .parts
            .iter()
            .filter(|part| part.thought.unwrap_or(false) == thought)
            .filter_map(|part| part.text.as_deref())
            .collect()
    }

    fn completed_content(
        final_content: Vec<ContentPart>,
        accumulated_text: Vec<ContentPart>,
        accumulated_thinking: String,
    ) -> Vec<ContentPart> {
        if accumulated_text.is_empty() && accumulated_thinking.is_empty() {
            return final_content;
        }
        let mut retained = Vec::new();
        for part in final_content {
            match part {
                ContentPart::Thinking { .. } if !accumulated_thinking.is_empty() => {}
                ContentPart::Text { .. } if !accumulated_text.is_empty() => {}
                other => retained.push(other),
            }
        }
        let mut content = Vec::new();
        if !accumulated_thinking.is_empty() {
            push_provider_thinking_content(&mut content, Some(accumulated_thinking));
        }
        content.extend(retained);
        content.extend(accumulated_text);
        content
    }
}

impl GeminiGroundingMetadata {
    fn append(&mut self, newer: Self) {
        // GroundingSupport indices address the stream-wide append order.
        self.grounding_chunks.extend(newer.grounding_chunks);
        self.grounding_supports.extend(newer.grounding_supports);
        self.web_search_queries.extend(newer.web_search_queries);
        self.retrieval_queries.extend(newer.retrieval_queries);
        if let Some(citations) = newer.citation_metadata {
            let retained = self.citation_metadata.get_or_insert_default();
            retained.citation_sources.extend(citations.citation_sources);
            retained.extra.extend(citations.extra);
        }
        if let Some(entry) = newer.search_entry_point {
            self.search_entry_point = Some(entry);
        }
        if let Some(token) = newer.google_maps_widget_context_token {
            self.google_maps_widget_context_token = Some(token);
        }
        self.extra.extend(newer.extra);
    }
}
