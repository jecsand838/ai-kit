use reqwest::multipart::Part;

use crate::SdkError;
use crate::error::ValidationViolation;
use crate::generation::ContentPart;

pub(crate) mod base64_bytes {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(value))
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        STANDARD.decode(value).map_err(serde::de::Error::custom)
    }
}

impl ContentPart {
    pub(crate) fn validate(&self) -> Result<(), SdkError> {
        match self {
            Self::Text { text } if text.trim().is_empty() => Err(SdkError::invalid_input(
                ValidationViolation::Empty {
                    field: "content.text",
                },
                "text content must not be blank",
            )),
            Self::Thinking { text: Some(text) } if text.trim().is_empty() => {
                Err(SdkError::invalid_input(
                    ValidationViolation::Empty {
                        field: "content.thinking",
                    },
                    "thinking content must not be blank when present",
                ))
            }
            Self::ImageUrl { url }
                if !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some() =>
            {
                Err(SdkError::invalid_input(
                    ValidationViolation::InvalidFormat {
                        field: "content.image_url",
                        rule: "HTTP(S) URL without credentials",
                    },
                    "remote image content requires an HTTP(S) URL without credentials",
                ))
            }
            Self::Image { media_type, data } => Self::validate_media("image", media_type, data),
            Self::Audio { media_type, data } => Self::validate_media("audio", media_type, data),
            Self::Document {
                media_type,
                data,
                file_name,
            } => {
                Self::validate_media("document", media_type, data)?;
                if file_name.as_ref().is_some_and(|name| {
                    name.trim().is_empty()
                        || name.trim() != name
                        || name.chars().any(char::is_control)
                }) {
                    return Err(SdkError::invalid_input(
                        ValidationViolation::InvalidFormat {
                            field: "content.document.file_name",
                            rule: "nonblank, unpadded, without control characters",
                        },
                        "document file name must be nonblank, unpadded, and contain no control characters",
                    ));
                }
                Ok(())
            }
            Self::File { reference } => reference.validate(),
            Self::Text { .. }
            | Self::Thinking { .. }
            | Self::ImageUrl { .. }
            | Self::Json { .. } => Ok(()),
        }
    }

    fn validate_media(kind: &'static str, media_type: &str, data: &[u8]) -> Result<(), SdkError> {
        let prefix_valid = kind == "document" || media_type.starts_with(&format!("{kind}/"));
        if media_type.trim() != media_type
            || !prefix_valid
            || Part::bytes(Vec::new()).mime_str(media_type).is_err()
        {
            return Err(SdkError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "content.media_type",
                    rule: "valid type/subtype compatible with content modality",
                },
                format!("{kind} media type must be a valid compatible type/subtype"),
            ));
        }
        if data.is_empty() {
            return Err(SdkError::invalid_input(
                ValidationViolation::Empty {
                    field: "content.data",
                },
                format!("inline {kind} data must not be empty"),
            ));
        }
        Ok(())
    }
}

pub(crate) fn text_part(part: &ContentPart) -> Result<Option<String>, SdkError> {
    match part {
        ContentPart::Text { text } => Ok(Some(text.clone())),
        ContentPart::Image { .. } => Ok(None),
        ContentPart::Thinking { .. } => Err(SdkError::invalid_input(
            ValidationViolation::MissingField {
                field: "message.continuation",
            },
            "thinking content requires assistant continuation state",
        )),
        other => Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "content",
                rule: "text conversion is unavailable for this modality",
            },
            format!("convert {other:?} to text"),
        )),
    }
}

pub(crate) fn text_parts(parts: &[ContentPart], separator: &str) -> Result<String, SdkError> {
    Ok(parts
        .iter()
        .map(text_part)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>()
        .join(separator))
}
