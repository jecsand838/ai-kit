use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;
use url::Url;

use crate::KitError;
use crate::error::{NativeErrorDetails, ResponseViolation};
use crate::generation::{
    Artifact, Citation, ContentPart, FileReference, FinishReason, FunctionCall, GenerateResponse,
    Message, Usage,
};
use crate::hosted::HostedToolOutput;
use crate::models::ModelId;
use crate::transport::CheckedRequestSpec;

#[derive(Default)]
pub(crate) struct NormalizedGenerationParts {
    pub(crate) content: Vec<ContentPart>,
    pub(crate) function_calls: Vec<FunctionCall>,
    pub(crate) citations: Vec<Citation>,
    pub(crate) artifacts: Vec<Artifact>,
    pub(crate) hosted_tool_outputs: Vec<HostedToolOutput>,
}

impl NormalizedGenerationParts {
    pub(crate) fn is_empty(&self) -> bool {
        self.content.is_empty()
            && self.function_calls.is_empty()
            && self.artifacts.is_empty()
            && self.hosted_tool_outputs.is_empty()
    }

    pub(crate) fn push_hosted_output(&mut self, output: HostedToolOutput) {
        match &output {
            HostedToolOutput::WebSearch { citations, .. }
            | HostedToolOutput::FileSearch { citations, .. }
            | HostedToolOutput::UrlContext { citations, .. } => {
                self.citations.extend(citations.clone());
            }
            HostedToolOutput::CodeExecution { artifacts, .. } => {
                self.artifacts.extend(artifacts.clone());
            }
        }
        self.hosted_tool_outputs.push(output);
    }

    pub(crate) fn extend_citations<I>(&mut self, citations: I)
    where
        I: IntoIterator<Item = Citation>,
    {
        self.citations.extend(citations);
    }
}

pub(crate) fn ensure_nonempty_generation_parts(
    parts: &NormalizedGenerationParts,
    body: Option<NativeErrorDetails>,
) -> Result<(), KitError> {
    if parts.is_empty() {
        return Err(KitError::invalid_response(ResponseViolation::ProtocolViolation { rule: "nonempty_generation" }).with_message("response did not include generated content, function calls, artifacts, or hosted output".to_string()).with_diagnostic(body));
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct GenerateResponseOptions {
    pub(crate) finish_reason: Option<FinishReason>,
    pub(crate) usage: Usage,
}

pub(crate) fn generate_response(
    model: ModelId,
    parts: NormalizedGenerationParts,
    options: GenerateResponseOptions,
) -> GenerateResponse {
    let NormalizedGenerationParts {
        content,
        function_calls,
        citations,
        artifacts,
        hosted_tool_outputs,
    } = parts;
    GenerateResponse {
        model,
        message: Message::returned_assistant(content, function_calls, None),
        finish_reason: options.finish_reason,
        citations,
        artifacts,
        hosted_tool_outputs,
        usage: options.usage,
    }
}

pub(crate) fn tool_call(
    id: Option<String>,
    name: Option<String>,
    arguments: Value,
) -> Result<FunctionCall, KitError> {
    let name = name.ok_or_else(|| {
        KitError::invalid_response(ResponseViolation::MissingField {
            field: "function.name",
        })
        .with_message("function call did not include a name".to_string())
        .with_diagnostic(CheckedRequestSpec::diagnostic_json(&arguments))
    })?;
    FunctionCall::new(id, name, arguments.clone()).map_err(|error| {
        KitError::invalid_response(ResponseViolation::InvalidField { field: "function" })
            .with_message("provider function call used an invalid id or name".to_string())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(&arguments))
            .with_supplementary(error)
    })
}

pub(crate) fn push_text_part(parts: &mut Vec<ContentPart>, text: Option<String>) {
    if let Some(text) = text
        && !text.is_empty()
    {
        parts.push(ContentPart::Text { text });
    }
}

pub(crate) fn push_provider_thinking_content(parts: &mut Vec<ContentPart>, text: Option<String>) {
    let text = text.filter(|value| !value.is_empty());
    if text.is_some() {
        parts.push(ContentPart::Thinking { text });
    }
}

pub(crate) fn inline_media_part(
    media_type: String,
    data_base64: String,
) -> Result<ContentPart, KitError> {
    let data = STANDARD.decode(data_base64).map_err(|err| {
        KitError::invalid_response(ResponseViolation::InvalidField {
            field: "inline_media.data",
        })
        .with_message(format!("provider inline media was not valid base64: {err}"))
        .with_body(None)
    })?;
    let part = if media_type.starts_with("image/") {
        ContentPart::Image { media_type, data }
    } else if media_type.starts_with("audio/") {
        ContentPart::Audio { media_type, data }
    } else {
        ContentPart::Document {
            media_type,
            data,
            file_name: None,
        }
    };
    part.validate().map_err(|error| {
        KitError::invalid_response(ResponseViolation::InvalidField {
            field: "inline_media",
        })
        .with_message("provider inline media used an invalid media type or empty body".to_string())
        .with_supplementary(error)
    })?;
    Ok(part)
}

pub(crate) fn file_reference(
    id: Option<String>,
    provider_path: Option<String>,
    media_type: Option<String>,
    url: Option<Url>,
) -> Result<FileReference, KitError> {
    let reference = if let Some(url) = url {
        FileReference::Url { url, media_type }
    } else {
        provider_path
            .or(id)
            .filter(|value| !value.trim().is_empty())
            .map(|id| FileReference::Provider { id, media_type })
            .ok_or_else(|| {
                KitError::invalid_response(ResponseViolation::MissingField {
                    field: "file_reference",
                })
                .with_message("file reference did not include an id, URI, or URL".to_string())
                .with_body(None)
            })?
    };
    reference.validate().map_err(|error| {
        KitError::invalid_response(ResponseViolation::InvalidField {
            field: "file_reference",
        })
        .with_message("provider returned an unusable file reference".to_string())
        .with_supplementary(error)
    })?;
    Ok(reference)
}

pub(crate) fn file_reference_part(
    id: Option<String>,
    provider_path: Option<String>,
    media_type: Option<String>,
    url: Option<Url>,
) -> Result<ContentPart, KitError> {
    Ok(ContentPart::File {
        reference: file_reference(id, provider_path, media_type, url)?,
    })
}

pub(crate) fn artifact(
    media_type: Option<String>,
    file_name: Option<String>,
    description: Option<String>,
    data_base64: Option<String>,
    reference: Option<FileReference>,
) -> Result<Artifact, KitError> {
    match (data_base64, reference) {
        (Some(data), None) => Ok(Artifact::Inline {
            media_type: media_type.ok_or_else(|| {
                KitError::invalid_response(ResponseViolation::MissingField {
                    field: "artifact.media_type",
                })
                .with_message("inline artifact did not include a media type".to_string())
                .with_body(None)
            })?,
            file_name,
            description,
            data: STANDARD.decode(data).map_err(|err| {
                KitError::invalid_response(ResponseViolation::InvalidField {
                    field: "artifact.data",
                })
                .with_message(format!("inline artifact was not valid base64: {err}"))
                .with_body(None)
            })?,
        }),
        (None, Some(reference)) => Ok(Artifact::File {
            reference,
            file_name,
            description,
        }),
        _ => Err(
            KitError::invalid_response(ResponseViolation::ProtocolViolation {
                rule: "one_artifact_source",
            })
            .with_message(
                "artifact must contain exactly one inline body or file reference".to_string(),
            )
            .with_body(None),
        ),
    }
}
