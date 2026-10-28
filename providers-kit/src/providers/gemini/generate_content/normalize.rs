use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::KitError;
use crate::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, NativeErrorDetails, RemoteFailure, ResponseDetails,
    ResponseViolation,
};

use crate::core::{
    GenerateResponseOptions, HostedJsonCursor, NormalizedGenerationParts,
    ensure_nonempty_generation_parts, file_reference_part, generate_response, inline_media_part,
    push_provider_thinking_content, push_text_part,
};
use crate::generation::FileReference;
use crate::generation::{Citation, FinishReason, GenerateResponse, GenerationContinuation, Usage};
use crate::hosted::{
    HostedCodeExecutionResult, HostedExecutableCode, HostedFileSearchResult, HostedToolOutput,
    HostedToolStatus, HostedWebSearchAction, HostedWebSearchResult,
};
use crate::models::ModelId;
use crate::transport::CheckedRequestSpec;

use super::super::wire::{
    GeminiContent, GeminiGenerateContentResponse, GeminiGroundingChunk, GeminiGroundingMetadata,
    GeminiGroundingSegment, GeminiPart, GeminiUrlContextMetadata, GeminiUsageMetadata,
};
use super::content::function_call_to_tool_call;

pub(crate) fn normalize_generate_response(
    selected_model: &ModelId,
    raw: Value,
) -> Result<GenerateResponse, KitError> {
    let signed_parts = raw
        .pointer("/candidates/0/content/parts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let parsed =
        serde_json::from_value::<GeminiGenerateContentResponse>(raw.clone()).map_err(|err| {
            KitError::invalid_response(ResponseViolation::InvalidField { field: "response" })
                .with_response_details(ResponseDetails::json(&err))
                .with_message(format!(
                    "failed to decode Gemini generateContent response: {err}"
                ))
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&raw))
                .at_phase(ErrorPhase::Decode)
        })?;
    parsed.into_response(selected_model, &raw, signed_parts)
}

impl GeminiContent {
    pub(crate) fn validate_model_role(&self) -> Result<(), KitError> {
        // Native Content.role is optional, and an empty role is also unset.
        // A returned candidate still cannot claim to be authored by the user.
        if !matches!(self.role.as_deref(), None | Some("" | "model")) {
            return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                field: "content.role",
            })
            .with_message("Gemini candidate content used a non-model role".to_string()));
        }
        Ok(())
    }
}

impl GeminiGenerateContentResponse {
    pub(crate) fn prompt_blocked(&self, raw: &Value) -> Result<bool, KitError> {
        let Some(reason) = self
            .prompt_feedback
            .as_ref()
            .and_then(|feedback| feedback.block_reason.as_deref())
        else {
            return Ok(false);
        };
        if !matches!(
            reason,
            "SAFETY" | "OTHER" | "BLOCKLIST" | "PROHIBITED_CONTENT" | "IMAGE_SAFETY"
        ) {
            return Err(
                KitError::invalid_response(ResponseViolation::UnknownVariant {
                    field: "promptFeedback.blockReason",
                })
                .with_message(format!(
                    "Gemini promptFeedback used unknown blockReason `{reason}`"
                ))
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
            );
        }
        if !self.candidates.is_empty() {
            return Err(
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "blocked prompt cannot contain candidates",
                })
                .with_message("Gemini blocked prompt response also included candidates".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
            );
        }
        Ok(true)
    }

    pub(crate) fn into_response(
        self,
        selected_model: &ModelId,
        raw: &Value,
        signed_parts: Vec<Value>,
    ) -> Result<GenerateResponse, KitError> {
        let model_version = self.model_version.as_deref().ok_or_else(|| {
            KitError::invalid_response(ResponseViolation::MissingField {
                field: "modelVersion",
            })
            .with_message(
                "Gemini generateContent response did not include modelVersion".to_string(),
            )
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
        })?;
        if model_version.is_empty()
            || model_version.len() > 256
            || model_version.trim() != model_version
            || model_version.chars().any(char::is_control)
        {
            return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                field: "modelVersion",
            })
            .with_message(
                "Gemini generateContent response used an invalid modelVersion".to_string(),
            )
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)));
        }
        if self.prompt_blocked(raw)? {
            return Ok(generate_response(
                selected_model.clone(),
                NormalizedGenerationParts::default(),
                GenerateResponseOptions {
                    finish_reason: Some(FinishReason::ContentFilter),
                    usage: usage_from_metadata(self.usage_metadata.as_ref()),
                },
            ));
        }
        let [candidate] = self.candidates.as_slice() else {
            return Err(
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "response must contain exactly one candidate",
                })
                .with_message(
                    "Gemini generateContent response must include exactly one candidate"
                        .to_string(),
                )
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
            );
        };
        if candidate.index.is_some_and(|index| index != 0) {
            return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                field: "candidates.index",
            })
            .with_message("Gemini generateContent candidate used a nonzero index".to_string())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)));
        }
        let mut finish_reason = candidate.finish_reason()?.ok_or_else(|| {
            KitError::invalid_response(ResponseViolation::MissingField {
                field: "finishReason",
            })
            .with_message("Gemini terminal candidate did not include finishReason".to_string())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
        })?;
        let mut converted = match candidate.content.as_ref() {
            Some(content) => {
                content.validate_model_role()?;
                convert_parts(&content.parts).map_err(|error| {
                    error.with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
                })?
            }
            None if matches!(
                finish_reason,
                FinishReason::ContentFilter | FinishReason::Error
            ) =>
            {
                NormalizedGenerationParts::default()
            }
            None => {
                return Err(KitError::invalid_response(ResponseViolation::MissingField {
                    field: "content",
                })
                .with_message("Gemini terminal candidate did not include content".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)));
            }
        };
        if !converted.function_calls.is_empty() && matches!(finish_reason, FinishReason::Stop) {
            finish_reason = FinishReason::ToolCall;
        }
        if let Some(grounding) = candidate.grounding_metadata.as_ref() {
            for output in hosted_outputs_from_grounding(grounding)? {
                converted.push_hosted_output(output);
            }
        }
        if let Some(url_context) = candidate.url_context_metadata.as_ref() {
            for output in url_context.hosted_outputs()? {
                converted.push_hosted_output(output);
            }
        }
        if !allows_empty_generation(&finish_reason) {
            ensure_nonempty_generation_parts(&converted, CheckedRequestSpec::diagnostic_json(raw))?;
        }
        let mut response = generate_response(
            selected_model.clone(),
            converted,
            GenerateResponseOptions {
                finish_reason: Some(finish_reason),
                usage: usage_from_metadata(self.usage_metadata.as_ref()),
            },
        );
        if !signed_parts.is_empty() {
            response.message =
                response
                    .message
                    .with_continuation(GenerationContinuation::gemini(
                        selected_model.clone(),
                        signed_parts,
                    ))?;
        }
        Ok(response)
    }
}

#[derive(Default)]
struct GeminiPartAccumulator {
    parts: NormalizedGenerationParts,
    code_activity: Vec<GeminiCodeActivity>,
}

impl GeminiPartAccumulator {
    fn push(&mut self, part: &GeminiPart) -> Result<(), KitError> {
        if part.thought.unwrap_or(false) {
            push_provider_thinking_content(&mut self.parts.content, part.text.clone());
        } else {
            push_text_part(&mut self.parts.content, part.text.clone());
        }
        self.push_media(part)?;
        if let Some(function_call) = &part.function_call {
            self.parts
                .function_calls
                .push(function_call_to_tool_call(function_call)?);
        }
        if let Some(value) = &part.executable_code {
            self.code_activity
                .push(GeminiCodeActivity::executable(value)?);
        }
        if let Some(value) = &part.code_execution_result {
            self.code_activity
                .push(GeminiCodeActivity::execution_result(value)?);
        }
        Ok(())
    }

    fn push_media(&mut self, part: &GeminiPart) -> Result<(), KitError> {
        if let Some(inline_data) = &part.inline_data {
            self.parts.content.push(inline_media_part(
                inline_data.mime_type.clone().ok_or_else(|| {
                    invalid_part_response(
                        part,
                        "inlineData.mimeType",
                        "Gemini inlineData did not include mimeType",
                    )
                })?,
                inline_data.data.clone().ok_or_else(|| {
                    invalid_part_response(
                        part,
                        "inlineData.data",
                        "Gemini inlineData did not include data",
                    )
                })?,
            )?);
        }
        if let Some(file_data) = &part.file_data {
            let file_uri = file_data.file_uri.clone().ok_or_else(|| {
                invalid_part_response(
                    part,
                    "fileData.fileUri",
                    "Gemini fileData did not include fileUri",
                )
            })?;
            let parsed_url = url::Url::parse(&file_uri).ok();
            self.parts.content.push(file_reference_part(
                None,
                Some(file_uri),
                file_data.mime_type.clone(),
                parsed_url,
            )?);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<NormalizedGenerationParts, KitError> {
        let mut sources = BTreeMap::new();
        let mut result_ids = BTreeSet::new();
        for (index, activity) in self.code_activity.iter().enumerate() {
            let duplicate = match activity {
                GeminiCodeActivity::Executable { id: Some(id), .. } => {
                    sources.insert(id.as_str(), index).is_some().then_some(id)
                }
                GeminiCodeActivity::Result { id: Some(id), .. } => {
                    (!result_ids.insert(id.as_str())).then_some(id)
                }
                GeminiCodeActivity::Executable { id: None, .. }
                | GeminiCodeActivity::Result { id: None, .. } => None,
            };
            if let Some(id) = duplicate {
                return Err(
                    KitError::invalid_response(ResponseViolation::ProtocolViolation {
                        rule: "code execution identifiers must identify one source and one result",
                    })
                    .with_response_details(ResponseDetails::Discriminator { actual: id.clone() })
                    .with_native_capture(NativeErrorDetails {
                        tool_call_id: Some(id.clone()),
                        ..Default::default()
                    }),
                );
            }
        }
        let mut anonymous_sources = Vec::new();
        let mut source_for_result = BTreeMap::new();
        for (index, activity) in self.code_activity.iter().enumerate() {
            match activity {
                GeminiCodeActivity::Executable { id: None, .. } => anonymous_sources.push(index),
                GeminiCodeActivity::Result { id: Some(id), .. } => {
                    if let Some(source) = sources.get(id.as_str()) {
                        source_for_result.insert(index, *source);
                    }
                }
                GeminiCodeActivity::Result { id: None, .. } if anonymous_sources.len() == 1 => {
                    if let Some(source) = anonymous_sources.pop() {
                        source_for_result.insert(index, source);
                    }
                }
                GeminiCodeActivity::Executable { id: Some(_), .. }
                | GeminiCodeActivity::Result { id: None, .. } => {}
            }
        }
        let mut outputs = BTreeMap::new();
        let mut paired_results = BTreeMap::new();
        for (index, activity) in self.code_activity.into_iter().enumerate() {
            let (code, results) = match activity {
                GeminiCodeActivity::Executable { code, .. } => (vec![code], Vec::new()),
                GeminiCodeActivity::Result { result, .. } => {
                    if let Some(source) = source_for_result.get(&index) {
                        paired_results.insert(*source, result);
                        continue;
                    }
                    (Vec::new(), vec![result])
                }
            };
            outputs.insert(
                index,
                HostedToolOutput::CodeExecution {
                    status: None,
                    code,
                    results,
                    artifacts: Vec::new(),
                },
            );
        }
        for (source, result) in paired_results {
            let Some(HostedToolOutput::CodeExecution { results, .. }) = outputs.get_mut(&source)
            else {
                return Err(KitError::invalid_response(
                    ResponseViolation::ProtocolViolation {
                        rule: "matched code result must retain its native source",
                    },
                ));
            };
            results.push(result);
        }
        for output in outputs.into_values() {
            self.parts.push_hosted_output(output);
        }
        Ok(self.parts)
    }
}

fn convert_parts(parts: &[GeminiPart]) -> Result<NormalizedGenerationParts, KitError> {
    let mut accumulator = GeminiPartAccumulator::default();
    for part in parts {
        accumulator.push(part)?;
    }
    accumulator.finish()
}

fn citation_from_grounding_chunk(
    chunk: &GeminiGroundingChunk,
    segment: &GeminiGroundingSegment,
) -> Result<Option<Citation>, KitError> {
    if let Some(web) = &chunk.web {
        let url = web.uri.clone().ok_or_else(|| {
            KitError::invalid_response(ResponseViolation::MissingField { field: "web.uri" })
                .with_message("Gemini web grounding chunk did not include a URI".to_string())
                .with_diagnostic(None)
        })?;
        return Ok(Some(Citation::Url {
            title: web.title.clone(),
            url,
            snippet: segment.text.clone(),
            start_index: segment.start_index,
            end_index: segment.end_index,
        }));
    }
    let retrieved = chunk.retrieved_context.as_ref().ok_or_else(|| {
        KitError::invalid_response(ResponseViolation::MissingField {
            field: "groundingChunks.source",
        })
        .with_message("Gemini grounding chunk did not include a supported source".to_string())
        .with_diagnostic(None)
    })?;
    let retrieved = GeminiRetrievedContextView::new(retrieved)?;
    let Some(id) = retrieved.file_id()? else {
        return Ok(None);
    };
    Ok(Some(Citation::File {
        reference: FileReference::Provider {
            id,
            media_type: None,
        },
        title: retrieved.optional_string("title")?,
        snippet: segment.text.clone().or(retrieved.optional_string("text")?),
        start_index: segment.start_index,
        end_index: segment.end_index,
    }))
}

fn validate_grounding_chunk(chunk: &GeminiGroundingChunk) -> Result<(), KitError> {
    if !chunk.extra.is_empty() {
        return Err(
            KitError::invalid_response(ResponseViolation::UnknownVariant {
                field: "groundingChunks.source",
            })
            .with_message("Gemini grounding chunk contained an unknown source family".to_string())
            .with_diagnostic(None),
        );
    }
    if chunk.maps.is_some() {
        return Err(
            KitError::invalid_response(ResponseViolation::UnknownVariant {
                field: "groundingChunks.maps",
            })
            .with_message("Gemini returned location grounding for a portable request".to_string())
            .with_diagnostic(None),
        );
    }
    if usize::from(chunk.web.is_some()) + usize::from(chunk.retrieved_context.is_some()) != 1 {
        return Err(
            KitError::invalid_response(ResponseViolation::ProtocolViolation {
                rule: "grounding chunk must contain exactly one source",
            })
            .with_message(
                "Gemini grounding chunk must include exactly one supported source".to_string(),
            )
            .with_diagnostic(None),
        );
    }
    Ok(())
}

fn hosted_outputs_from_grounding(
    grounding: &GeminiGroundingMetadata,
) -> Result<Vec<HostedToolOutput>, KitError> {
    grounding.validate_search_entry_point()?;
    if grounding.google_maps_widget_context_token.is_some() {
        return Err(
            KitError::invalid_response(ResponseViolation::UnknownVariant {
                field: "groundingMetadata.maps",
            })
            .with_message("Gemini returned unsupported Google Maps grounding metadata".to_string())
            .with_diagnostic(None),
        );
    }
    if !grounding.extra.is_empty() {
        return Err(
            KitError::invalid_response(ResponseViolation::UnknownVariant {
                field: "groundingMetadata",
            })
            .with_message(format!(
                "Gemini returned unsupported grounding metadata field(s): {}",
                grounding
                    .extra
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
            .with_diagnostic(None),
        );
    }
    for chunk in &grounding.grounding_chunks {
        validate_grounding_chunk(chunk)?;
    }
    validate_referenced_chunks(grounding)?;
    let mut outputs = Vec::new();
    if grounding
        .grounding_chunks
        .iter()
        .any(|chunk| chunk.web.is_some())
        || !grounding.web_search_queries.is_empty()
    {
        outputs.push(grounding.web_search_output()?);
    }
    if grounding
        .grounding_chunks
        .iter()
        .any(|chunk| chunk.retrieved_context.is_some())
    {
        outputs.push(file_search_output(grounding)?);
    }
    Ok(outputs)
}

impl GeminiGroundingMetadata {
    fn validate_search_entry_point(&self) -> Result<(), KitError> {
        let Some(value) = self.search_entry_point.as_ref() else {
            return Ok(());
        };
        let Some(object) = value.as_object() else {
            return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                field: "searchEntryPoint",
            })
            .with_message("Gemini searchEntryPoint must be an object"));
        };
        if object.is_empty()
            || object.iter().any(|(name, value)| {
                !matches!(name.as_str(), "renderedContent" | "sdkBlob") || !value.is_string()
            })
        {
            return Err(KitError::invalid_response(ResponseViolation::InvalidField { field: "searchEntryPoint" })
                .with_message("Gemini searchEntryPoint must contain only string renderedContent or sdkBlob fields"));
        }
        Ok(())
    }

    fn web_search_output(&self) -> Result<HostedToolOutput, KitError> {
        let action = (!self.web_search_queries.is_empty()).then(|| HostedWebSearchAction::Search {
            query: None,
            queries: self.web_search_queries.clone(),
            sources: Vec::new(),
        });
        let results = self
            .grounding_chunks
            .iter()
            .filter_map(|chunk| chunk.web.as_ref())
            .map(|web| {
                Ok(HostedWebSearchResult {
                    title: web.title.clone(),
                    url: web.uri.clone().ok_or_else(|| {
                        KitError::invalid_response(ResponseViolation::MissingField {
                            field: "web.uri",
                        })
                        .with_message(
                            "Gemini web grounding chunk did not include a URI".to_string(),
                        )
                        .with_diagnostic(None)
                    })?,
                    snippet: None,
                })
            })
            .collect::<Result<Vec<_>, KitError>>()?;
        Ok(HostedToolOutput::WebSearch {
            status: None,
            action,
            results,
            citations: citations_from_grounding(self, GeminiGroundingFamily::Web)?,
        })
    }
}

fn validate_referenced_chunks(grounding: &GeminiGroundingMetadata) -> Result<(), KitError> {
    for support in &grounding.grounding_supports {
        for index in &support.grounding_chunk_indices {
            if *index >= grounding.grounding_chunks.len() {
                return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                    field: "groundingChunkIndices",
                })
                .with_response_details(ResponseDetails::Index {
                    index: *index,
                    available: grounding.grounding_chunks.len(),
                })
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(grounding)));
            }
        }
    }
    Ok(())
}

fn file_search_output(grounding: &GeminiGroundingMetadata) -> Result<HostedToolOutput, KitError> {
    let mut results = Vec::new();
    for retrieved in grounding
        .grounding_chunks
        .iter()
        .filter_map(|chunk| chunk.retrieved_context.as_ref())
    {
        let retrieved = GeminiRetrievedContextView::new(retrieved)?;
        let file_name = retrieved.optional_string("title")?;
        let text = retrieved.optional_string("text")?;
        if let Some(file_id) = retrieved.file_id()? {
            results.push(HostedFileSearchResult {
                file_id,
                file_name,
                text,
                score: None,
            });
        }
    }
    Ok(HostedToolOutput::FileSearch {
        status: None,
        queries: grounding.retrieval_queries.clone(),
        results: Some(results),
        citations: citations_from_grounding(grounding, GeminiGroundingFamily::Retrieved)?,
    })
}

struct GeminiRetrievedContextView<'a> {
    raw: &'a Value,
    object: &'a Map<String, Value>,
}

impl<'a> GeminiRetrievedContextView<'a> {
    fn new(raw: &'a Value) -> Result<Self, KitError> {
        let object = raw.as_object().ok_or_else(|| {
            KitError::invalid_response(ResponseViolation::InvalidField {
                field: "retrievedContext",
            })
            .with_message("Gemini retrievedContext must be a JSON object".to_owned())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
        })?;
        Ok(Self { raw, object })
    }

    fn optional_string(&self, field: &'static str) -> Result<Option<String>, KitError> {
        match self.object.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(other) => {
                Err(
                    KitError::invalid_response(ResponseViolation::InvalidField { field })
                        .with_message(format!("Gemini retrievedContext.{field} must be a string"))
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(other)),
                )
            }
        }
    }

    fn file_id(&self) -> Result<Option<String>, KitError> {
        match self.object.get("uri") {
            None => Ok(None),
            Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
            Some(_) => Err(KitError::invalid_response(ResponseViolation::InvalidField {
                field: "retrievedContext.uri",
            })
            .with_message("Gemini retrievedContext.uri must be a nonblank string".to_owned())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(self.raw))),
        }
    }
}

#[derive(Clone, Copy)]
enum GeminiGroundingFamily {
    Web,
    Retrieved,
}

impl GeminiGroundingFamily {
    fn matches(self, chunk: &GeminiGroundingChunk) -> bool {
        match self {
            Self::Web => chunk.web.is_some(),
            Self::Retrieved => chunk.retrieved_context.is_some(),
        }
    }
}

fn citations_from_grounding(
    grounding: &GeminiGroundingMetadata,
    family: GeminiGroundingFamily,
) -> Result<Vec<Citation>, KitError> {
    let mut citations = Vec::new();
    for support in &grounding.grounding_supports {
        let Some(segment) = support.segment.as_ref() else {
            continue;
        };
        for index in &support.grounding_chunk_indices {
            let chunk = &grounding.grounding_chunks[*index];
            if family.matches(chunk)
                && let Some(citation) = citation_from_grounding_chunk(chunk, segment)?
            {
                citations.push(citation);
            }
        }
    }
    Ok(citations)
}

impl GeminiUrlContextMetadata {
    fn hosted_outputs(&self) -> Result<Vec<HostedToolOutput>, KitError> {
        self.url_metadata
            .iter()
            .map(|metadata| {
                let status = match metadata.url_retrieval_status.as_deref() {
                    None | Some("URL_RETRIEVAL_STATUS_UNSPECIFIED") => None,
                    Some("URL_RETRIEVAL_STATUS_SUCCESS") => Some(HostedToolStatus::Completed),
                    Some(
                        outcome @ ("URL_RETRIEVAL_STATUS_ERROR"
                        | "URL_RETRIEVAL_STATUS_PAYWALL"
                        | "URL_RETRIEVAL_STATUS_UNSAFE"),
                    ) => {
                        let kind = if outcome == "URL_RETRIEVAL_STATUS_ERROR" {
                            ErrorKind::ProviderRejected
                        } else {
                            ErrorKind::PermissionDenied
                        };
                        Some(HostedToolStatus::Failed {
                            error: RemoteFailure::from_native(
                                kind,
                                ClassificationBasis::NativeOutcome,
                                NativeErrorDetails {
                                    outcome: Some(outcome.into()),
                                    url: metadata.retrieved_url.clone(),
                                    ..CheckedRequestSpec::diagnostic_json(metadata)
                                        .unwrap_or_default()
                                },
                            )?,
                        })
                    }
                    Some(actual) => {
                        return Err(KitError::invalid_response(
                            ResponseViolation::UnknownVariant {
                                field: "urlRetrievalStatus",
                            },
                        )
                        .with_response_details(ResponseDetails::Discriminator {
                            actual: actual.to_owned(),
                        })
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(metadata)));
                    }
                };
                let citations = if matches!(status, Some(HostedToolStatus::Completed)) {
                    metadata
                        .retrieved_url
                        .as_ref()
                        .map(|url| Citation::Url {
                            url: url.clone(),
                            title: None,
                            snippet: None,
                            start_index: None,
                            end_index: None,
                        })
                        .into_iter()
                        .collect()
                } else {
                    Vec::new()
                };
                Ok(HostedToolOutput::UrlContext {
                    requested_url: None,
                    retrieved_url: metadata.retrieved_url.clone(),
                    status,
                    citations,
                })
            })
            .collect()
    }
}

enum GeminiCodeActivity {
    Executable {
        id: Option<String>,
        code: HostedExecutableCode,
    },
    Result {
        id: Option<String>,
        result: HostedCodeExecutionResult,
    },
}

impl GeminiCodeActivity {
    fn executable(value: &Value) -> Result<Self, KitError> {
        let cursor = HostedJsonCursor::new(
            "executableCode",
            CheckedRequestSpec::diagnostic_json(value).unwrap_or_default(),
            value,
        );
        let language = cursor.required_str_field("language")?;
        if language != "PYTHON" {
            return Err(cursor
                .invalid(
                    ResponseViolation::UnknownVariant {
                        field: "executableCode.language",
                    },
                    "unsupported code language",
                )
                .with_response_details(ResponseDetails::Discriminator {
                    actual: language.to_owned(),
                }));
        }
        Ok(Self::Executable {
            id: cursor.optional_str_field("id")?.filter(|id| !id.is_empty()),
            code: HostedExecutableCode {
                language: Some(language.to_owned()),
                code: cursor.required_str_field("code")?.to_owned(),
            },
        })
    }

    fn execution_result(value: &Value) -> Result<Self, KitError> {
        let cursor = HostedJsonCursor::new(
            "codeExecutionResult",
            CheckedRequestSpec::diagnostic_json(value).unwrap_or_default(),
            value,
        );
        let id = cursor.optional_str_field("id")?.filter(|id| !id.is_empty());
        let outcome = cursor.required_str_field("outcome")?;
        let output = cursor.optional_str_field("output")?;
        let (status, stdout, logs) = match outcome {
            "OUTCOME_OK" => (HostedToolStatus::Completed, output, None),
            "OUTCOME_FAILED" | "OUTCOME_DEADLINE_EXCEEDED" => {
                let kind = if outcome == "OUTCOME_DEADLINE_EXCEEDED" {
                    ErrorKind::DeadlineExceeded
                } else {
                    ErrorKind::ProviderRejected
                };
                let failure = RemoteFailure::from_native(
                    kind,
                    ClassificationBasis::NativeOutcome,
                    NativeErrorDetails {
                        outcome: Some(outcome.to_owned()),
                        tool_call_id: id.clone(),
                        ..CheckedRequestSpec::diagnostic_json(value).unwrap_or_default()
                    },
                )?;
                (HostedToolStatus::Failed { error: failure }, None, output)
            }
            actual => {
                return Err(cursor
                    .invalid(
                        ResponseViolation::UnknownVariant {
                            field: "codeExecutionResult.outcome",
                        },
                        "unsupported execution outcome",
                    )
                    .with_response_details(ResponseDetails::Discriminator {
                        actual: actual.to_owned(),
                    }));
            }
        };
        Ok(Self::Result {
            id,
            result: HostedCodeExecutionResult {
                status: Some(status),
                stdout,
                logs,
                stderr: None,
                exit_code: None,
            },
        })
    }
}

pub(crate) fn usage_from_metadata(metadata: Option<&GeminiUsageMetadata>) -> Usage {
    let Some(metadata) = metadata else {
        return Usage::default();
    };
    Usage::reported_tokens(metadata.prompt_token_count, metadata.candidates_token_count)
        .with_reported_total(metadata.total_token_count)
        .with_cached_input(metadata.cached_content_token_count)
        .with_reasoning(metadata.thoughts_token_count)
}

fn allows_empty_generation(reason: &FinishReason) -> bool {
    matches!(
        reason,
        FinishReason::Length | FinishReason::ContentFilter | FinishReason::Error
    )
}

fn invalid_part_response(
    part: &GeminiPart,
    field: &'static str,
    message: &'static str,
) -> KitError {
    KitError::invalid_response(ResponseViolation::MissingField { field })
        .with_message(message.to_string())
        .with_diagnostic(CheckedRequestSpec::diagnostic_json(part))
}
