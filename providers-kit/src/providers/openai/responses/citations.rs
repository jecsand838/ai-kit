use crate::KitError;
use crate::error::{ErrorPhase, ResponseViolation};
use crate::generation::{Citation, FileReference};

use super::super::wire::{OpenAiAnnotation, OpenAiContentItem};

pub(super) fn content_items_to_citations(
    items: &[OpenAiContentItem],
) -> Result<Vec<Citation>, KitError> {
    let mut citations = Vec::new();
    for item in items {
        for annotation in &item.annotations {
            citations.push(annotation_to_citation(annotation)?);
        }
    }
    Ok(citations)
}

fn annotation_to_citation(annotation: &OpenAiAnnotation) -> Result<Citation, KitError> {
    let fields = (
        annotation.title.clone(),
        annotation.text.clone(),
        annotation.start_index,
        annotation.end_index,
    );
    if let Some(url) = annotation.url.clone() {
        Ok(Citation::Url {
            url,
            title: fields.0,
            snippet: fields.1,
            start_index: fields.2,
            end_index: fields.3,
        })
    } else {
        let id = annotation
            .file_id
            .clone()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                KitError::invalid_response(ResponseViolation::MissingField {
                    field: "annotation.reference",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message(format!(
                    "OpenAI annotation `{}` did not include a URL or file ID",
                    annotation.kind
                ))
                .with_diagnostic(None)
            })?;
        Ok(Citation::File {
            reference: FileReference::Provider {
                id,
                media_type: None,
            },
            title: fields.0,
            snippet: fields.1,
            start_index: fields.2,
            end_index: fields.3,
        })
    }
}
