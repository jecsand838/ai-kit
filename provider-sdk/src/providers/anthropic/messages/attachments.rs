use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use crate::SdkError;
use crate::error::ValidationViolation;
use crate::generation::{ContentPart, FileReference};

pub(super) fn user_content_part_to_block(part: &ContentPart) -> Result<Value, SdkError> {
    match part {
        ContentPart::Text { text } => Ok(json!({"type": "text", "text": text})),
        ContentPart::ImageUrl { url } => Ok(json!({
            "type": "image",
            "source": {"type": "url", "url": url},
        })),
        ContentPart::Image { media_type, data } => Ok(json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": STANDARD.encode(data),
            },
        })),
        ContentPart::Document {
            media_type,
            data,
            file_name,
        } => {
            let mut block = json!({
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": STANDARD.encode(data),
                },
            });
            if let Some(file_name) = file_name {
                block["title"] = Value::String(file_name.clone());
            }
            Ok(block)
        }
        ContentPart::File { reference } => file_reference_to_block(reference),
        ContentPart::Thinking { .. } => Err(SdkError::invalid_input(
            ValidationViolation::MissingField {
                field: "message.continuation",
            },
            "Anthropic thinking input requires provider continuation on an assistant message",
        )),
        ContentPart::Audio { .. } => Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "content.audio",
                rule: "audio input unavailable",
            },
            "Anthropic Messages audio input",
        )),
        ContentPart::Json { .. } => Err(SdkError::invalid_input(
            ValidationViolation::InvalidValue {
                field: "content.json",
                rule: "JSON content requires a function result",
            },
            "Anthropic JSON content is valid only inside a function result",
        )),
    }
}

pub(super) fn assistant_content_part_to_block(part: &ContentPart) -> Result<Value, SdkError> {
    match part {
        ContentPart::Text { text } => Ok(json!({"type": "text", "text": text})),
        ContentPart::Thinking { .. } => Err(SdkError::invalid_input(
            ValidationViolation::MissingField {
                field: "message.continuation",
            },
            "Anthropic thinking input must be replayed through provider continuation",
        )),
        _ => Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "message.content",
                rule: "assistant history requires text or continued thinking",
            },
            "Anthropic assistant history content other than text and continued thinking",
        )),
    }
}

pub(super) fn tool_result_content_part_to_block(part: &ContentPart) -> Result<Value, SdkError> {
    match part {
        ContentPart::Text { text } => Ok(json!({"type": "text", "text": text})),
        ContentPart::Json { value } => Ok(json!({"type": "text", "text": value.to_string()})),
        ContentPart::ImageUrl { url } => Ok(json!({
            "type": "image",
            "source": {"type": "url", "url": url},
        })),
        ContentPart::Image { media_type, data } => Ok(json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": STANDARD.encode(data),
            },
        })),
        ContentPart::Document { .. } | ContentPart::File { .. } => user_content_part_to_block(part),
        ContentPart::Thinking { .. } | ContentPart::Audio { .. } => Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "function_result.content",
                rule: "thinking and audio unavailable",
            },
            "Anthropic function-result thinking or audio content",
        )),
    }
}

fn file_reference_to_block(reference: &FileReference) -> Result<Value, SdkError> {
    let media_type = match reference {
        FileReference::Provider { media_type, .. } | FileReference::Url { media_type, .. } => {
            media_type.as_deref().ok_or_else(|| {
                SdkError::invalid_input(
                    ValidationViolation::MissingField {
                        field: "file_reference.media_type",
                    },
                    "Anthropic file references require a media type",
                )
            })?
        }
    };
    let kind = match media_type {
        "image/jpeg" | "image/png" | "image/gif" | "image/webp" => "image",
        "application/pdf" | "text/plain" => "document",
        _ => {
            return Err(SdkError::unsupported(
                ValidationViolation::Unsupported {
                    field: "file_reference.media_type",
                    rule: "unsupported media type",
                },
                format!("Anthropic file media type `{media_type}`"),
            ));
        }
    };
    let source = match reference {
        FileReference::Provider { id, .. } => json!({"type": "file", "file_id": id}),
        FileReference::Url { url, .. } => json!({"type": "url", "url": url}),
    };
    Ok(json!({
        "type": kind,
        "source": source,
    }))
}
