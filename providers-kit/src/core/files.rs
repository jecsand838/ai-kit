use std::num::NonZeroUsize;

use reqwest::multipart::Part;

use crate::KitError;
use crate::error::ValidationViolation;

pub(crate) fn multipart_file_part(
    file_name: String,
    media_type: String,
    data: Vec<u8>,
) -> Result<Part, KitError> {
    Part::bytes(data)
        .file_name(file_name)
        .mime_str(&media_type)
        .map_err(|err| {
            KitError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "file.media_type",
                    rule: "valid MIME type",
                },
                format!("invalid upload media type: {err}"),
            )
        })
}

pub(crate) fn limit_after_path(
    base: &str,
    limit: Option<NonZeroUsize>,
    cursor: Option<&str>,
) -> String {
    let mut path = base.to_string();
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    if let Some(limit) = limit {
        query.append_pair("limit", &limit.to_string());
    }
    if let Some(cursor) = cursor {
        query.append_pair("after", cursor);
    }
    let query = query.finish();
    if !query.is_empty() {
        path.push('?');
        path.push_str(&query);
    }
    path
}
