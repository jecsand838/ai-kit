use serde_json::{Map, Number, Value};

use crate::KitError;
use crate::error::{
    ErrorKind, ErrorPhase, JsonValueKind, Operation, ResponseDetails, ResponseViolation,
};
use crate::generation::{Citation, FileReference};

use super::super::wire::AnthropicCitation;

impl AnthropicCitation {
    pub(crate) fn decode(raw: &Value) -> Result<Self, KitError> {
        let object = raw
            .as_object()
            .ok_or_else(|| Self::field_error("citation", JsonValueKind::Object, Some(raw)))?;
        match Self::required_str(object, "type", "citation.type")? {
            "web_search_result_location" => {
                Self::required_str(object, "encrypted_index", "citation.encrypted_index")?;
                Ok(Self::WebSearch {
                    url: Self::required_str(object, "url", "citation.url")?.to_owned(),
                    title: Self::optional_string(object, "title", "citation.title")?,
                    cited_text: Self::required_str(object, "cited_text", "citation.cited_text")?
                        .to_owned(),
                })
            }
            "char_location" => Ok(Self::Character {
                document_index: Self::required_integer(
                    object,
                    "document_index",
                    "citation.document_index",
                )?,
                start_char_index: Self::required_integer(
                    object,
                    "start_char_index",
                    "citation.start_char_index",
                )?,
                end_char_index: Self::required_integer(
                    object,
                    "end_char_index",
                    "citation.end_char_index",
                )?,
                file_id: Self::optional_string(object, "file_id", "citation.file_id")?,
                document_title: Self::optional_string(
                    object,
                    "document_title",
                    "citation.document_title",
                )?,
                cited_text: Self::required_str(object, "cited_text", "citation.cited_text")?
                    .to_owned(),
            }),
            "page_location" => Ok(Self::Page {
                document_index: Self::required_integer(
                    object,
                    "document_index",
                    "citation.document_index",
                )?,
                start_page_number: Self::required_integer(
                    object,
                    "start_page_number",
                    "citation.start_page_number",
                )?,
                end_page_number: Self::required_integer(
                    object,
                    "end_page_number",
                    "citation.end_page_number",
                )?,
                file_id: Self::optional_string(object, "file_id", "citation.file_id")?,
                document_title: Self::optional_string(
                    object,
                    "document_title",
                    "citation.document_title",
                )?,
                cited_text: Self::required_str(object, "cited_text", "citation.cited_text")?
                    .to_owned(),
            }),
            "content_block_location" => Ok(Self::ContentBlock {
                document_index: Self::required_integer(
                    object,
                    "document_index",
                    "citation.document_index",
                )?,
                start_block_index: Self::required_integer(
                    object,
                    "start_block_index",
                    "citation.start_block_index",
                )?,
                end_block_index: Self::required_integer(
                    object,
                    "end_block_index",
                    "citation.end_block_index",
                )?,
                file_id: Self::optional_string(object, "file_id", "citation.file_id")?,
                document_title: Self::optional_string(
                    object,
                    "document_title",
                    "citation.document_title",
                )?,
                cited_text: Self::required_str(object, "cited_text", "citation.cited_text")?
                    .to_owned(),
            }),
            "search_result_location" => {
                Self::required_str(object, "cited_text", "citation.cited_text")?;
                Self::required_str(object, "source", "citation.source")?;
                Self::optional_string(object, "title", "citation.title")?;
                Ok(Self::SearchResult {
                    search_result_index: Self::required_integer(
                        object,
                        "search_result_index",
                        "citation.search_result_index",
                    )?,
                    start_block_index: Self::required_integer(
                        object,
                        "start_block_index",
                        "citation.start_block_index",
                    )?,
                    end_block_index: Self::required_integer(
                        object,
                        "end_block_index",
                        "citation.end_block_index",
                    )?,
                })
            }
            actual => Err(
                KitError::invalid_response(ResponseViolation::UnknownVariant {
                    field: "citation.type",
                })
                .with_response_details(ResponseDetails::Discriminator {
                    actual: actual.to_owned(),
                }),
            ),
        }
    }

    pub(crate) fn into_portable(self) -> Result<Citation, KitError> {
        match self {
            Self::WebSearch {
                url,
                title,
                cited_text,
            } => Ok(Citation::Url {
                url,
                title,
                snippet: Some(cited_text),
                start_index: None,
                end_index: None,
            }),
            Self::Character {
                document_index,
                start_char_index,
                end_char_index,
                file_id,
                document_title,
                cited_text,
            } => {
                Self::source_index(&document_index, "citation.document_index")?;
                let start = Self::source_index(&start_char_index, "citation.start_char_index")?;
                let end = Self::source_index(&end_char_index, "citation.end_char_index")?;
                if end < start {
                    return Err(Self::invalid_index("citation.end_char_index"));
                }
                Self::file_citation(file_id, document_title, cited_text)
            }
            Self::Page {
                document_index,
                start_page_number,
                end_page_number,
                file_id,
                document_title,
                cited_text,
            } => {
                Self::source_index(&document_index, "citation.document_index")?;
                let start = Self::source_index(&start_page_number, "citation.start_page_number")?;
                let end = Self::source_index(&end_page_number, "citation.end_page_number")?;
                if start == 0 {
                    return Err(Self::invalid_index("citation.start_page_number"));
                }
                if end == 0 || end < start {
                    return Err(Self::invalid_index("citation.end_page_number"));
                }
                Self::file_citation(file_id, document_title, cited_text)
            }
            Self::ContentBlock {
                document_index,
                start_block_index,
                end_block_index,
                file_id,
                document_title,
                cited_text,
            } => {
                Self::source_index(&document_index, "citation.document_index")?;
                let start = Self::source_index(&start_block_index, "citation.start_block_index")?;
                let end = Self::source_index(&end_block_index, "citation.end_block_index")?;
                if end <= start {
                    return Err(Self::invalid_index("citation.end_block_index"));
                }
                Self::file_citation(file_id, document_title, cited_text)
            }
            Self::SearchResult {
                search_result_index,
                start_block_index,
                end_block_index,
            } => {
                Self::source_index(&search_result_index, "citation.search_result_index")?;
                let start = Self::source_index(&start_block_index, "citation.start_block_index")?;
                let end = Self::source_index(&end_block_index, "citation.end_block_index")?;
                if end <= start {
                    return Err(Self::invalid_index("citation.end_block_index"));
                }
                Err(KitError::new(
                    ErrorKind::Unsupported,
                    Operation::Unspecified,
                    ErrorPhase::Normalize,
                )
                .with_message("Anthropic indexed search-result citations"))
            }
        }
    }

    fn required_str<'a>(
        object: &'a Map<String, Value>,
        key: &'static str,
        field: &'static str,
    ) -> Result<&'a str, KitError> {
        match object.get(key) {
            Some(Value::String(value)) => Ok(value),
            actual => Err(Self::field_error(field, JsonValueKind::String, actual)),
        }
    }

    fn optional_string(
        object: &Map<String, Value>,
        key: &'static str,
        field: &'static str,
    ) -> Result<Option<String>, KitError> {
        match object.get(key) {
            Some(Value::String(value)) => Ok(Some(value.clone())),
            None | Some(Value::Null) => Ok(None),
            actual => Err(Self::field_error(field, JsonValueKind::String, actual)),
        }
    }

    fn required_integer(
        object: &Map<String, Value>,
        key: &'static str,
        field: &'static str,
    ) -> Result<Number, KitError> {
        match object.get(key) {
            Some(Value::Number(value)) if value.is_i64() || value.is_u64() => Ok(value.clone()),
            actual => Err(Self::field_error(field, JsonValueKind::Number, actual)),
        }
    }

    fn field_error(
        field: &'static str,
        expected: JsonValueKind,
        actual: Option<&Value>,
    ) -> KitError {
        let violation = if actual.is_none() {
            ResponseViolation::MissingField { field }
        } else {
            ResponseViolation::InvalidField { field }
        };
        KitError::invalid_response(violation).with_response_details(ResponseDetails::FieldType {
            expected,
            actual: JsonValueKind::of(actual),
        })
    }

    fn source_index(value: &Number, field: &'static str) -> Result<u64, KitError> {
        value.as_u64().ok_or_else(|| Self::invalid_index(field))
    }

    fn invalid_index(field: &'static str) -> KitError {
        KitError::invalid_response(ResponseViolation::InvalidField { field }).with_response_details(
            ResponseDetails::FieldType {
                expected: JsonValueKind::Number,
                actual: JsonValueKind::Number,
            },
        )
    }

    fn file_citation(
        file_id: Option<String>,
        title: Option<String>,
        cited_text: String,
    ) -> Result<Citation, KitError> {
        let id = file_id.ok_or_else(|| {
            KitError::new(
                ErrorKind::Unsupported,
                Operation::Unspecified,
                ErrorPhase::Normalize,
            )
            .with_message("Anthropic indexed document citations without a native file ID")
        })?;
        Ok(Citation::File {
            reference: FileReference::Provider {
                id,
                media_type: None,
            },
            title,
            snippet: Some(cited_text),
            start_index: None,
            end_index: None,
        })
    }
}
