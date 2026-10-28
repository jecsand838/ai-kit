use crate::KitError;
use crate::error::ValidationViolation;
use crate::generation::ContentPart;

pub(crate) fn tool_result_text(parts: &[ContentPart]) -> Result<String, KitError> {
    parts
        .iter()
        .map(|part| match part {
            ContentPart::Text { text } => Ok(text.clone()),
            ContentPart::Json { value } => Ok(value.to_string()),
            ContentPart::Thinking { .. }
            | ContentPart::Audio { .. }
            | ContentPart::Image { .. }
            | ContentPart::ImageUrl { .. }
            | ContentPart::Document { .. }
            | ContentPart::File { .. } => Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "function_result.content",
                    rule: "requires text-compatible content",
                },
                "render non-text function result",
            )),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("\n"))
}
