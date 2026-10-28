use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use crate::KitError;
use crate::error::ValidationViolation;
use crate::generation::{ContentPart, FileReference};

pub(super) fn parts_to_wire_parts(parts: &[ContentPart]) -> Result<Vec<Value>, KitError> {
    parts.iter().map(part_to_wire_part).collect()
}

pub(super) fn part_to_wire_part(part: &ContentPart) -> Result<Value, KitError> {
    match part {
        ContentPart::Text { text } => Ok(json!({"text": text})),
        ContentPart::Thinking { text: Some(text) } => Ok(json!({"text": text, "thought": true})),
        ContentPart::Thinking { text: None } => Err(KitError::invalid_input(
            ValidationViolation::MissingField {
                field: "content.thinking.text",
            },
            "Gemini thinking content requires text",
        )),
        ContentPart::ImageUrl { url } => file_data(url.as_str(), None),
        ContentPart::Image { media_type, data } | ContentPart::Audio { media_type, data } => {
            inline_data(media_type, data)
        }
        ContentPart::Document {
            media_type, data, ..
        } => inline_data(media_type, data),
        ContentPart::File { reference } => match reference {
            FileReference::Provider { .. } => Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "file_reference",
                    rule: "uploaded media requires its URL reference",
                },
                "Gemini generation requires the uploaded file URL from FileObject::input_reference",
            )),
            FileReference::Url { url, media_type } => {
                file_data(url.as_str(), media_type.as_deref())
            }
        },
        ContentPart::Json { .. } => Err(KitError::invalid_input(
            ValidationViolation::InvalidValue {
                field: "content.json",
                rule: "JSON content requires a function result",
            },
            "JSON content is valid only inside a Gemini function result",
        )),
    }
}

fn inline_data(media_type: &str, data: &[u8]) -> Result<Value, KitError> {
    if media_type.trim().is_empty() {
        return Err(KitError::invalid_input(
            ValidationViolation::Empty {
                field: "content.media_type",
            },
            "inline media type must not be empty",
        ));
    }
    Ok(json!({
        "inlineData": {
            "mimeType": media_type,
            "data": STANDARD.encode(data),
        }
    }))
}

fn file_data(uri: &str, media_type: Option<&str>) -> Result<Value, KitError> {
    if uri.trim().is_empty() {
        return Err(KitError::invalid_input(
            ValidationViolation::Empty {
                field: "file_reference.url",
            },
            "Gemini file URI must not be empty",
        ));
    }
    let mut data = serde_json::Map::new();
    data.insert("fileUri".to_string(), Value::String(uri.to_string()));
    if let Some(media_type) = media_type {
        data.insert(
            "mimeType".to_string(),
            Value::String(media_type.to_string()),
        );
    }
    Ok(json!({"fileData": data}))
}
