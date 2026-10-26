use crate::SdkError;
use crate::error::ValidationViolation;
use crate::generation::FileReference;

impl FileReference {
    pub(crate) fn validate(&self) -> Result<(), SdkError> {
        let media_type = match self {
            Self::Provider { id, media_type } => {
                if id.trim().is_empty() || id.trim() != id || id.chars().any(char::is_control) {
                    return Err(SdkError::invalid_input(
                        ValidationViolation::InvalidFormat {
                            field: "file_reference.id",
                            rule: "nonblank, unpadded, without control characters",
                        },
                        "provider file references must be nonblank, unpadded, and contain no control characters",
                    ));
                }
                media_type
            }
            Self::Url { url, media_type } => {
                if !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                {
                    return Err(SdkError::invalid_input(
                        ValidationViolation::InvalidFormat {
                            field: "file_reference.url",
                            rule: "HTTP(S) URL without credentials",
                        },
                        "remote file references require an HTTP(S) URL without credentials",
                    ));
                }
                media_type
            }
        };
        if let Some(media_type) = media_type
            && (media_type.trim() != media_type
                || reqwest::multipart::Part::bytes(Vec::new())
                    .mime_str(media_type)
                    .is_err())
        {
            return Err(SdkError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "file_reference.media_type",
                    rule: "valid type/subtype",
                },
                "file reference media type must be a valid type/subtype",
            ));
        }
        Ok(())
    }
}
